//! Benchmark for HTTP web seed download performance.
//!
//! This benchmark measures the throughput of downloading torrents from HTTP web seeds.
//!
//! # Environment Variables
//! - `RQBIT_BENCH_SIZE_GB`: Data size in GB (default: 50)
//! - `RQBIT_BENCH_SIZE_MB`: Data size in MB (overrides SIZE_GB if set)
//! - `RQBIT_BENCH_CACHE`: Cache directory for test data (default: system temp)
//! - `RQBIT_BENCH_PORT`: HTTP server port (default: 0 for auto-assign)
//! - `RQBIT_BENCH_PIECE_SIZE_MB`: Piece size in MB (default: 16)
//! - `RQBIT_BENCH_NUM_FILES`: Number of files to create (default: 10)
//!
//! # Running
//! ```bash
//! # Quick test with 100MB
//! RQBIT_BENCH_SIZE_MB=100 cargo bench --bench webseed_benchmark -p librqbit
//!
//! # Test with 1GB
//! RQBIT_BENCH_SIZE_GB=1 cargo bench --bench webseed_benchmark -p librqbit
//!
//! # Full 50GB benchmark
//! cargo bench --bench webseed_benchmark -p librqbit
//!
//! # With custom cache directory
//! RQBIT_BENCH_CACHE=/data/bench_cache cargo bench --bench webseed_benchmark -p librqbit
//! ```

use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::Body,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use bencode::bencode_serialize_to_writer;
use buffers::ByteBufOwned;
use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use librqbit::{AddTorrent, CreateTorrentOptions, Session, SessionOptions, create_torrent};
use rand::{RngCore, SeedableRng};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::io::ReaderStream;

// ============ Configuration ============

#[derive(Clone, Debug)]
struct BenchConfig {
    /// Total size of test data in bytes
    data_size: u64,
    /// Piece length for torrent
    piece_length: u32,
    /// Number of files to split data into
    num_files: usize,
    /// HTTP server port (0 for auto-assign)
    server_port: u16,
    /// Cache directory for test data
    cache_dir: PathBuf,
}

impl BenchConfig {
    fn from_env() -> Self {
        // Support both GB (default) and MB for smaller test sizes
        let data_size: u64 = if let Ok(mb) = std::env::var("RQBIT_BENCH_SIZE_MB") {
            mb.parse::<u64>().unwrap_or(100) * 1024 * 1024
        } else {
            let gb: u64 = std::env::var("RQBIT_BENCH_SIZE_GB")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(50);
            gb * 1024 * 1024 * 1024
        };

        let piece_size_mb: u32 = std::env::var("RQBIT_BENCH_PIECE_SIZE_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16);

        let num_files: usize = std::env::var("RQBIT_BENCH_NUM_FILES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        let server_port: u16 = std::env::var("RQBIT_BENCH_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0); // 0 = auto-assign

        let cache_dir = std::env::var("RQBIT_BENCH_CACHE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join("rqbit_webseed_bench"));

        Self {
            data_size,
            piece_length: piece_size_mb * 1024 * 1024,
            num_files,
            server_port,
            cache_dir,
        }
    }

    fn config_hash(&self) -> String {
        format!(
            "size_{}_piece_{}_files_{}",
            self.data_size, self.piece_length, self.num_files
        )
    }
}

// ============ Data Generation ============

fn create_random_file(path: &Path, size: u64) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .expect("Failed to create file");

    const BUF_SIZE: usize = 1024 * 1024; // 1MB buffer
    let mut rng = rand::rngs::SmallRng::from_os_rng();
    let mut buffer = vec![0u8; BUF_SIZE];
    let mut remaining = size;

    while remaining > 0 {
        let to_write = remaining.min(BUF_SIZE as u64) as usize;
        rng.fill_bytes(&mut buffer[..to_write]);
        file.write_all(&buffer[..to_write])
            .expect("Failed to write");
        remaining -= to_write as u64;
    }
}

fn setup_test_data(config: &BenchConfig) -> PathBuf {
    let data_dir = config.cache_dir.join(config.config_hash());
    let marker_file = data_dir.join(".complete");

    // Check if data already exists
    if marker_file.exists() {
        eprintln!("Using cached test data at {:?}", data_dir);
        return data_dir;
    }

    eprintln!(
        "Creating {} of test data in {:?}...",
        format_size(config.data_size),
        data_dir
    );

    // Create directory
    std::fs::create_dir_all(&data_dir).expect("Failed to create data directory");

    // Create files
    let file_size = config.data_size / config.num_files as u64;
    for i in 0..config.num_files {
        let file_path = data_dir.join(format!("file_{:03}.bin", i));
        eprintln!(
            "  Creating file {}/{}: {}",
            i + 1,
            config.num_files,
            format_size(file_size)
        );
        create_random_file(&file_path, file_size);
    }

    // Mark as complete
    std::fs::write(&marker_file, "complete").expect("Failed to write marker");

    eprintln!("Test data created successfully");
    data_dir
}

fn format_size(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{} bytes", bytes)
    }
}

// ============ Streaming HTTP Server ============

struct WebSeedServerState {
    root_dir: PathBuf,
}

/// Parse HTTP Range header "bytes=start-end"
fn parse_range_header(header: &str, total_size: u64) -> Option<(u64, u64)> {
    let header = header.strip_prefix("bytes=")?;
    let parts: Vec<&str> = header.split('-').collect();
    if parts.len() != 2 {
        return None;
    }

    let start: u64 = parts[0].parse().ok()?;
    let end: u64 = if parts[1].is_empty() {
        total_size - 1
    } else {
        parts[1].parse().ok()?
    };

    if start <= end && end < total_size {
        Some((start, end))
    } else {
        None
    }
}

/// Streaming file server with HTTP Range support
async fn serve_file_streaming(
    State(state): State<Arc<WebSeedServerState>>,
    AxumPath(path): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    let file_path = state.root_dir.join(&path);

    // Open file
    let file = match tokio::fs::File::open(&file_path).await {
        Ok(f) => f,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let metadata = match file.metadata().await {
        Ok(m) => m,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let total_size = metadata.len();

    // Check for Range header
    if let Some(range_header) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_header.to_str() {
            if let Some((start, end)) = parse_range_header(range_str, total_size) {
                let content_length = end - start + 1;

                // For range requests, use streaming reader with seek
                let body = Body::from_stream(LimitedRangeStream::new(
                    file_path.clone(),
                    start,
                    content_length,
                ));

                return (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        (
                            header::CONTENT_TYPE,
                            "application/octet-stream".to_string(),
                        ),
                        (header::CONTENT_LENGTH, content_length.to_string()),
                        (
                            header::CONTENT_RANGE,
                            format!("bytes {}-{}/{}", start, end, total_size),
                        ),
                        (header::ACCEPT_RANGES, "bytes".to_string()),
                    ],
                    body,
                )
                    .into_response();
            }
        }
    }

    // Full file response
    let stream = ReaderStream::new(tokio::io::BufReader::new(file));
    let body = Body::from_stream(stream);

    (
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                "application/octet-stream".to_string(),
            ),
            (header::CONTENT_LENGTH, total_size.to_string()),
            (header::ACCEPT_RANGES, "bytes".to_string()),
        ],
        body,
    )
        .into_response()
}

/// Stream that reads a specific range from a file
struct LimitedRangeStream {
    file_path: PathBuf,
    start: u64,
    remaining: u64,
}

impl LimitedRangeStream {
    fn new(file_path: PathBuf, start: u64, length: u64) -> Self {
        Self {
            file_path,
            start,
            remaining: length,
        }
    }
}

impl futures::Stream for LimitedRangeStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        if self.remaining == 0 {
            return Poll::Ready(None);
        }

        // We need to handle async file operations
        // For simplicity, use a blocking approach wrapped in spawn_blocking
        // In a real implementation, you'd want proper async file I/O

        let file_path = self.file_path.clone();
        let start = self.start;
        let remaining = self.remaining;

        // Read chunk
        let chunk_size = remaining.min(64 * 1024) as usize; // 64KB chunks

        // This is a simplified sync implementation
        // For production, use proper async I/O
        match std::fs::File::open(&file_path) {
            Ok(mut file) => {
                use std::io::{Read, Seek, SeekFrom};
                if let Err(e) = file.seek(SeekFrom::Start(start)) {
                    return Poll::Ready(Some(Err(e)));
                }

                let mut buf = vec![0u8; chunk_size];
                match file.read(&mut buf) {
                    Ok(n) => {
                        if n == 0 {
                            return Poll::Ready(None);
                        }
                        buf.truncate(n);
                        self.start += n as u64;
                        self.remaining -= n as u64;
                        Poll::Ready(Some(Ok(Bytes::from(buf))))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

fn create_webseed_router(state: Arc<WebSeedServerState>) -> Router {
    Router::new()
        .route("/{*path}", get(serve_file_streaming))
        .with_state(state)
}

async fn start_webseed_server(root_dir: PathBuf, port: u16) -> anyhow::Result<(u16, tokio::task::JoinHandle<()>)> {
    let state = Arc::new(WebSeedServerState { root_dir });
    let router = create_webseed_router(state);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    let actual_port = listener.local_addr()?.port();

    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    Ok((actual_port, handle))
}

// ============ Torrent Helpers ============

async fn create_torrent_with_webseed(
    data_dir: &Path,
    piece_length: u32,
    webseed_url: String,
) -> anyhow::Result<Bytes> {
    let result = create_torrent(
        data_dir,
        CreateTorrentOptions {
            piece_length: Some(piece_length),
            ..Default::default()
        },
    )
    .await?;

    let mut meta = result.meta.clone();
    meta.url_list = vec![ByteBufOwned::from(webseed_url.into_bytes())];

    let mut buf = Vec::new();
    bencode_serialize_to_writer(&meta, &mut buf)?;
    Ok(Bytes::from(buf))
}

// ============ Benchmark ============

fn webseed_throughput_benchmark(c: &mut Criterion) {
    // Initialize logging
    if std::env::var("RUST_LOG").is_err() {
        unsafe { std::env::set_var("RUST_LOG", "warn") };
    }
    let _ = tracing_subscriber::fmt::try_init();

    let config = BenchConfig::from_env();
    eprintln!("Benchmark config: {:?}", config);

    // Create tokio runtime
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Setup test data (one-time, cached)
    let data_dir = setup_test_data(&config);

    // Start HTTP server
    let (server_port, server_handle) = rt.block_on(async {
        // Serve from parent directory since URL will include data_dir name
        let server_root = data_dir.parent().unwrap().to_path_buf();
        start_webseed_server(server_root, config.server_port)
            .await
            .expect("Failed to start server")
    });

    eprintln!("HTTP server listening on port {}", server_port);

    // Create torrent file
    let webseed_url = format!("http://127.0.0.1:{}/", server_port);
    let torrent_bytes = rt.block_on(async {
        create_torrent_with_webseed(&data_dir, config.piece_length, webseed_url)
            .await
            .expect("Failed to create torrent")
    });

    eprintln!("Torrent created, starting benchmark...");

    let mut group = c.benchmark_group("webseed_download");

    // Configure for long-running benchmarks
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(300));
    group.throughput(criterion::Throughput::Bytes(config.data_size));

    let bench_name = format!("{}_download", format_size(config.data_size).replace(" ", "_").replace(".", "_"));

    group.bench_function(&bench_name, |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let torrent_bytes = torrent_bytes.clone();

            async move {
                let mut total_duration = Duration::ZERO;

                for _i in 0..iters {
                    // Create fresh output directory
                    let output_dir = TempDir::with_prefix("webseed_bench_output").unwrap();

                    // Create session with web-seed-only config
                    let session = Session::new_with_opts(
                        output_dir.path().into(),
                        SessionOptions {
                            disable_dht: true,
                            disable_dht_persistence: true,
                            disable_local_service_discovery: true,
                            persistence: None,
                            listen: None,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("Failed to create session");

                    let start = Instant::now();

                    // Add torrent and wait for completion
                    let handle = session
                        .add_torrent(AddTorrent::TorrentFileBytes(torrent_bytes.clone()), None)
                        .await
                        .expect("Failed to add torrent")
                        .into_handle()
                        .expect("Failed to get handle");

                    handle
                        .wait_until_completed()
                        .await
                        .expect("Download failed");

                    let elapsed = start.elapsed();
                    total_duration += elapsed;

                    // Cleanup
                    drop(session);
                }

                total_duration
            }
        });
    });

    group.finish();

    // Cleanup server
    server_handle.abort();
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .with_output_color(true);
    targets = webseed_throughput_benchmark
);
criterion_main!(benches);
