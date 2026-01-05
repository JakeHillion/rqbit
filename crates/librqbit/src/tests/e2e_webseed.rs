//! Integration tests for HTTP web seeds (BEP-19)

use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use bencode::bencode_serialize_to_writer;
use buffers::ByteBufOwned;
use bytes::Bytes;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tracing::info;

use crate::{
    AddTorrent, CreateTorrentOptions, Session, SessionOptions,
    create_torrent,
    create_torrent_file::CreateTorrentResult,
    tests::test_util::{create_default_random_dir_with_torrents, setup_test_logging},
    torrent_state::ManagedTorrentHandle,
};

// ============ HTTP Server with Range Support ============

struct WebSeedServerState {
    /// Root directory containing files to serve
    root_dir: PathBuf,
    /// Paths that should return 404
    not_found_paths: HashSet<String>,
    /// Counter for requests (for flaky server tests)
    request_count: AtomicU32,
    /// Number of requests to fail before succeeding
    fail_first_n: u32,
}

impl WebSeedServerState {
    fn new(root_dir: PathBuf) -> Self {
        Self {
            root_dir,
            not_found_paths: HashSet::new(),
            request_count: AtomicU32::new(0),
            fail_first_n: 0,
        }
    }

    fn with_fail_first_n(mut self, n: u32) -> Self {
        self.fail_first_n = n;
        self
    }
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

/// Handler that serves files with HTTP Range request support (BEP-19 compliant)
async fn serve_file_with_range(
    State(state): State<Arc<WebSeedServerState>>,
    AxumPath(path): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    // Check for flaky server behavior
    let request_num = state.request_count.fetch_add(1, Ordering::SeqCst);
    if request_num < state.fail_first_n {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }

    // Check for simulated 404s
    if state.not_found_paths.contains(&path) {
        return StatusCode::NOT_FOUND.into_response();
    }

    // Construct full file path
    let file_path = state.root_dir.join(&path);

    // Read file
    let file_bytes = match std::fs::read(&file_path) {
        Ok(bytes) => bytes,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    let total_size = file_bytes.len() as u64;

    // Parse Range header
    if let Some(range_header) = headers.get(header::RANGE) {
        if let Ok(range_str) = range_header.to_str() {
            if let Some((start, end)) = parse_range_header(range_str, total_size) {
                let content_length = end - start + 1;
                let body_bytes = file_bytes[start as usize..=end as usize].to_vec();

                return (
                    StatusCode::PARTIAL_CONTENT,
                    [
                        (header::CONTENT_TYPE, "application/octet-stream".to_string()),
                        (header::CONTENT_LENGTH, content_length.to_string()),
                        (
                            header::CONTENT_RANGE,
                            format!("bytes {}-{}/{}", start, end, total_size),
                        ),
                        (header::ACCEPT_RANGES, "bytes".to_string()),
                    ],
                    body_bytes,
                )
                    .into_response();
            }
        }
    }

    // Full file response
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (header::CONTENT_LENGTH, total_size.to_string()),
            (header::ACCEPT_RANGES, "bytes".to_string()),
        ],
        file_bytes,
    )
        .into_response()
}

fn create_webseed_router(state: Arc<WebSeedServerState>) -> Router {
    Router::new()
        .route("/{*path}", get(serve_file_with_range))
        .with_state(state)
}

async fn start_webseed_server(
    root_dir: PathBuf,
    port: u16,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let state = Arc::new(WebSeedServerState::new(root_dir));
    let router = create_webseed_router(state);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    info!("Web seed server listening on port {}", port);

    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    Ok(handle)
}

async fn start_webseed_server_with_state(
    state: Arc<WebSeedServerState>,
    port: u16,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    let router = create_webseed_router(state);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    info!("Web seed server listening on port {}", port);

    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.ok();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    Ok(handle)
}

// ============ Torrent Helpers ============

/// Create torrent bytes with url_list populated for web seeds
fn create_torrent_with_webseed(
    result: &CreateTorrentResult,
    webseed_urls: Vec<String>,
) -> anyhow::Result<Bytes> {
    let mut meta = result.meta.clone();
    meta.url_list = webseed_urls
        .into_iter()
        .map(|url| ByteBufOwned::from(url.into_bytes()))
        .collect();

    let mut buf = Vec::new();
    bencode_serialize_to_writer(&meta, &mut buf)?;
    Ok(Bytes::from(buf))
}

/// Get the torrent name from a CreateTorrentResult
fn get_torrent_name(result: &CreateTorrentResult) -> String {
    result
        .meta
        .info
        .data
        .name
        .as_ref()
        .map(|n| String::from_utf8_lossy(n.as_ref()).to_string())
        .unwrap_or_default()
}

/// Verify torrent completed successfully by checking stats
fn verify_torrent_completed(handle: &ManagedTorrentHandle) -> anyhow::Result<()> {
    let stats = handle.stats();

    // Check that the torrent is finished
    if !stats.finished {
        anyhow::bail!("Torrent not marked as finished");
    }

    // Check that all bytes are downloaded
    if let Some(live) = &stats.live {
        if live.snapshot.downloaded_and_checked_bytes != stats.total_bytes {
            anyhow::bail!(
                "Downloaded bytes mismatch: have {} bytes, expected {} bytes",
                live.snapshot.downloaded_and_checked_bytes,
                stats.total_bytes
            );
        }
    }

    info!(
        "Torrent completed successfully: {} bytes downloaded",
        stats.total_bytes
    );

    Ok(())
}

// ============ Integration Tests ============

#[tokio::test(flavor = "multi_thread")]
async fn test_webseed_single_file_download() {
    setup_test_logging();

    let port = 17001u16;

    // 1. Create source files
    let source_dir = create_default_random_dir_with_torrents(1, 256 * 1024, Some("webseed_single"));

    // 2. Create torrent
    let torrent = create_torrent(
        source_dir.path(),
        CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let torrent_name = get_torrent_name(&torrent);
    info!("Created torrent with name: {}", torrent_name);

    // 3. Create torrent with webseed URL
    let webseed_url = format!("http://127.0.0.1:{}/", port);
    let torrent_bytes = create_torrent_with_webseed(&torrent, vec![webseed_url]).unwrap();

    // 4. Start HTTP web seed server
    // Serve from PARENT directory since URL includes torrent_name/file_path
    let server_root = source_dir.path().parent().unwrap().to_path_buf();
    let server = start_webseed_server(server_root, port).await.unwrap();

    // 5. Create client session (no peers, only web seed)
    let client_dir = TempDir::with_prefix("webseed_client").unwrap();
    let session = Session::new_with_opts(
        client_dir.path().into(),
        SessionOptions {
            disable_dht: true,
            persistence: None,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // 6. Add torrent and wait for completion
    let handle = session
        .add_torrent(AddTorrent::TorrentFileBytes(torrent_bytes), None)
        .await
        .unwrap()
        .into_handle()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(60), handle.wait_until_completed())
        .await
        .expect("timeout waiting for download")
        .expect("download failed");

    // 7. Verify torrent completed
    verify_torrent_completed(&handle).unwrap();

    server.abort();
    info!("test_webseed_single_file_download passed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webseed_multi_file_download() {
    setup_test_logging();

    let port = 17002u16;

    // Create 3 files with sizes that span piece boundaries
    let source_dir = create_default_random_dir_with_torrents(3, 100_000, Some("webseed_multi"));

    let torrent = create_torrent(
        source_dir.path(),
        CreateTorrentOptions {
            piece_length: Some(32768), // 32KB pieces to force cross-file chunks
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let torrent_name = get_torrent_name(&torrent);
    info!("Created multi-file torrent with name: {}", torrent_name);

    let webseed_url = format!("http://127.0.0.1:{}/", port);
    let torrent_bytes = create_torrent_with_webseed(&torrent, vec![webseed_url]).unwrap();

    // Serve from PARENT directory since URL includes torrent_name/file_path
    let server_root = source_dir.path().parent().unwrap().to_path_buf();
    let server = start_webseed_server(server_root, port).await.unwrap();

    let client_dir = TempDir::with_prefix("webseed_multi_client").unwrap();
    let session = Session::new_with_opts(
        client_dir.path().into(),
        SessionOptions {
            disable_dht: true,
            persistence: None,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let handle = session
        .add_torrent(AddTorrent::TorrentFileBytes(torrent_bytes), None)
        .await
        .unwrap()
        .into_handle()
        .unwrap();

    tokio::time::timeout(Duration::from_secs(60), handle.wait_until_completed())
        .await
        .expect("timeout waiting for download")
        .expect("download failed");

    // Verify torrent completed
    verify_torrent_completed(&handle).unwrap();

    server.abort();
    info!("test_webseed_multi_file_download passed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webseed_404_permanent_failure() {
    setup_test_logging();

    let port = 17003u16;

    // Create a small file
    let source_dir =
        create_default_random_dir_with_torrents(1, 32 * 1024, Some("webseed_404_test"));

    let torrent = create_torrent(source_dir.path(), Default::default())
        .await
        .unwrap();

    let webseed_url = format!("http://127.0.0.1:{}/", port);
    let torrent_bytes = create_torrent_with_webseed(&torrent, vec![webseed_url]).unwrap();

    // Start server that points to wrong directory (will 404)
    let wrong_dir = TempDir::with_prefix("webseed_wrong_dir").unwrap();
    let server = start_webseed_server(wrong_dir.path().to_path_buf(), port)
        .await
        .unwrap();

    let client_dir = TempDir::with_prefix("webseed_404_client").unwrap();
    let session = Session::new_with_opts(
        client_dir.path().into(),
        SessionOptions {
            disable_dht: true,
            persistence: None,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let handle = session
        .add_torrent(AddTorrent::TorrentFileBytes(torrent_bytes), None)
        .await
        .unwrap()
        .into_handle()
        .unwrap();

    // Wait for web seed to fail and be marked dead
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify the torrent is NOT completed (web seed failed, no peers)
    let stats = handle.stats();
    assert!(
        !stats.finished,
        "Torrent should not be finished with 404 web seed"
    );
    assert!(
        stats.live.is_some(),
        "Torrent should be live (not completed)"
    );

    server.abort();
    info!("test_webseed_404_permanent_failure passed");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_webseed_backoff_on_temporary_error() {
    setup_test_logging();

    let port = 17004u16;

    // Create a small file
    let source_dir = create_default_random_dir_with_torrents(1, 64 * 1024, Some("webseed_backoff"));

    let torrent = create_torrent(
        source_dir.path(),
        CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let torrent_name = get_torrent_name(&torrent);

    let webseed_url = format!("http://127.0.0.1:{}/", port);
    let torrent_bytes = create_torrent_with_webseed(&torrent, vec![webseed_url]).unwrap();

    // Start server that fails first 5 requests with 500, then succeeds
    // Serve from PARENT directory since URL includes torrent_name/file_path
    let server_root = source_dir.path().parent().unwrap().to_path_buf();
    let state = Arc::new(WebSeedServerState::new(server_root).with_fail_first_n(5));
    let server = start_webseed_server_with_state(state, port)
        .await
        .unwrap();

    let client_dir = TempDir::with_prefix("webseed_backoff_client").unwrap();
    let session = Session::new_with_opts(
        client_dir.path().into(),
        SessionOptions {
            disable_dht: true,
            persistence: None,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let handle = session
        .add_torrent(AddTorrent::TorrentFileBytes(torrent_bytes), None)
        .await
        .unwrap()
        .into_handle()
        .unwrap();

    // Should eventually complete after retries (with backoff)
    // Give more time due to exponential backoff (1s, 2s, 4s, 8s...)
    tokio::time::timeout(Duration::from_secs(120), handle.wait_until_completed())
        .await
        .expect("timeout - download should complete after server starts responding")
        .expect("download failed");

    // Verify torrent completed
    verify_torrent_completed(&handle).unwrap();

    server.abort();
    info!("test_webseed_backoff_on_temporary_error passed");
}
