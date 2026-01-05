use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use librqbit_core::lengths::{ChunkInfo, Lengths, ValidPieceIndex};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::{
    chunk_tracker::ChunkTracker,
    session::WebSeedPriority,
    torrent_state::live::TorrentStateLive,
};

use super::{WebSeedDownloader, downloader::is_permanent_failure};

/// Spawns a task that continuously downloads chunks from a web seed
pub async fn task_webseed_chunk_requester(
    state: Arc<TorrentStateLive>,
    web_seed_url: String,
    downloader: Arc<WebSeedDownloader>,
    priority: WebSeedPriority,
) -> crate::Result<()> {
    info!("Starting web seed chunk requester for: {}", web_seed_url);

    // Track last downloaded piece for sequential downloading
    let mut last_downloaded_piece: Option<ValidPieceIndex> = None;

    loop {
        // If torrent is finished, we don't need web seeds anymore
        if state.is_finished_and_no_active_streams() {
            debug!("Torrent finished, stopping web seed requester for: {}", web_seed_url);
            return Ok(());
        }

        // Check if this web seed is in a good state to try
        if !state.web_seeds.seeds.get(&web_seed_url)
            .map(|seed| seed.backoff.should_retry())
            .unwrap_or(false)
        {
            // Backoff period, wait a bit before retrying
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Find a piece to download
        let piece_to_download = {
            let locked = state.locked.read();
            let chunks = match locked.get_chunks() {
                Ok(c) => c,
                Err(_) => {
                    debug!("Chunk tracker empty, stopping web seed requester");
                    return Ok(());
                }
            };

            find_piece_for_webseed(chunks, &state.peers, priority, &state.lengths, last_downloaded_piece)
        };

        let piece_index = match piece_to_download {
            Some(idx) => idx,
            None => {
                // No pieces to download right now, wait for new pieces
                tokio::select! {
                    _ = state.new_pieces_notify.notified() => {
                        continue;
                    }
                    _ = sleep(Duration::from_secs(5)) => {
                        continue;
                    }
                }
            }
        };

        // Try to download this piece
        match download_piece_from_webseed(
            &state,
            &downloader,
            &web_seed_url,
            piece_index,
        ).await {
            Ok(()) => {
                state.web_seeds.mark_success(&web_seed_url);
                last_downloaded_piece = Some(piece_index);
                debug!("Successfully downloaded piece {} from web seed: {}", piece_index, web_seed_url);
            }
            Err(e) => {
                // Check if this is a permanent or temporary failure
                let is_permanent = e.downcast_ref::<reqwest::Error>()
                    .and_then(|re| re.status())
                    .map(|s| is_permanent_failure(&s))
                    .unwrap_or(false);

                if is_permanent {
                    warn!("Permanent failure for web seed {}: {:#}", web_seed_url, e);
                    state.web_seeds.mark_failure(&web_seed_url, true);
                    return Ok(()); // Stop trying this web seed
                } else {
                    warn!("Temporary failure for web seed {}: {:#}", web_seed_url, e);
                    state.web_seeds.mark_failure(&web_seed_url, false);
                    state.web_seeds.record_request_failure(&web_seed_url);
                    // Will retry after backoff
                }
            }
        }
    }
}

/// Find a piece that should be downloaded from a web seed based on the 3-tier priority
/// If last_downloaded_piece is Some(n), will first try piece n+1 for sequential download
fn find_piece_for_webseed(
    chunks: &ChunkTracker,
    peers: &crate::torrent_state::live::peers::PeerStates,
    priority: WebSeedPriority,
    lengths: &Lengths,
    last_downloaded_piece: Option<ValidPieceIndex>,
) -> Option<ValidPieceIndex> {
    let hns = chunks.get_hns();
    if hns.needed_bytes == 0 {
        return None;
    }

    let have_pieces = chunks.get_have_pieces().as_slice();

    // Fast path: Try next sequential piece first (optimizes for common case)
    if let Some(last_piece) = last_downloaded_piece {
        let next_piece_idx = last_piece.get() + 1;
        if next_piece_idx < lengths.total_pieces() {
            if let Some(next_piece) = lengths.validate_piece_index(next_piece_idx) {
                if let Some(have_bit) = have_pieces.get(next_piece_idx as usize) {
                    if !*have_bit {
                        // Piece is needed, return it immediately for sequential download
                        return Some(next_piece);
                    }
                }
            }
        }
    }

    // Slow path: Collect candidate pieces and select randomly to avoid worker contention
    let mut tier1_candidates = Vec::new();
    let mut tier2_candidates = Vec::new();

    // Tier 1: Pieces NOT available from any peer (highest priority)
    for piece_idx in 0..lengths.total_pieces() {
        let piece_index = lengths.validate_piece_index(piece_idx)?;

        if *have_pieces.get(piece_idx as usize)? {
            continue;
        }

        let available_from_peers = peers.states.iter().any(|entry| {
            entry.value().get_live()
                .and_then(|live| live.bitfield.get(piece_idx as usize).map(|b| *b))
                .unwrap_or(false)
        });

        if !available_from_peers {
            tier1_candidates.push(piece_index);
        } else {
            tier2_candidates.push(piece_index);
        }
    }

    // Return random piece from tier 1 if any
    if !tier1_candidates.is_empty() {
        use rand::Rng;
        let idx = rand::rng().random_range(0..tier1_candidates.len());
        let selected = tier1_candidates[idx];
        debug!("Selected random piece {} from {} tier-1 candidates (unavailable from peers)", selected, tier1_candidates.len());
        return Some(selected);
    }

    // Tier 2: Based on configured priority
    match priority {
        WebSeedPriority::WebSeedsFirst | WebSeedPriority::Balanced => {
            // Return random piece from tier 2 candidates
            if !tier2_candidates.is_empty() {
                use rand::Rng;
                let idx = rand::rng().random_range(0..tier2_candidates.len());
                let selected = tier2_candidates[idx];
                debug!("Selected random piece {} from {} tier-2 candidates", selected, tier2_candidates.len());
                return Some(selected);
            }
        }
        WebSeedPriority::PeersFirst => {
            // Only use web seeds for tier 1 pieces (unavailable from peers)
            return None;
        }
    }

    None
}

/// Download a complete piece from a web seed and write it to disk
async fn download_piece_from_webseed(
    state: &TorrentStateLive,
    downloader: &WebSeedDownloader,
    web_seed_url: &str,
    piece_index: ValidPieceIndex,
) -> anyhow::Result<()> {
    // Lock the piece so no one else tries to download it
    let _piece_lock = state.per_piece_locks[piece_index.get_usize()].write();

    // Double-check we still need this piece
    {
        let locked = state.locked.read();
        let chunks = locked.get_chunks()?;
        let have_pieces = chunks.get_have_pieces().as_slice();
        if let Some(bit) = have_pieces.get(piece_index.get() as usize) {
            if *bit {
                return Ok(()); // Someone else already got it
            }
        }
    }

    debug!("Downloading piece {} from web seed: {}", piece_index, web_seed_url);

    // Download all chunks for this piece and write them
    let chunks: Vec<_> = state.lengths.iter_chunk_infos(piece_index).collect();

    // Drop the piece lock before async operations
    drop(_piece_lock);

    for chunk_info in chunks {
        let chunk_bytes = downloader
            .download_chunk(web_seed_url, chunk_info)
            .await
            .context("Failed to download chunk from web seed")?;

        // Write chunk to storage using the absolute offset
        write_chunk_to_storage(&state.files, &state.metadata.file_infos, &chunk_info, &chunk_bytes, &state.lengths)
            .context("Failed to write chunk to disk")?;
    }

    // Verify the piece hash
    let hash_ok = state
        .file_ops()
        .check_piece(piece_index)
        .context("Piece hash check failed")?;

    if !hash_ok {
        anyhow::bail!("Piece {} hash verification failed", piece_index);
    }

    // Mark the piece as complete (only update stats if we were the one to mark it)
    let was_newly_completed = {
        let mut locked = state.locked.write();
        let chunks = locked.get_chunks_mut()?;
        let have_pieces = chunks.get_have_pieces().as_slice();
        let already_had = have_pieces.get(piece_index.get() as usize).map(|b| *b).unwrap_or(true);
        if !already_had {
            chunks.mark_piece_downloaded(piece_index);
        }
        !already_had
    };

    if was_newly_completed {
        // Update global piece counters (same as peer downloads)
        let piece_len = state.lengths.piece_length(piece_index) as u64;
        state
            .stats
            .downloaded_and_checked_bytes
            .fetch_add(piece_len, std::sync::atomic::Ordering::Release);
        state
            .stats
            .downloaded_and_checked_pieces
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        state
            .stats
            .have_bytes
            .fetch_add(piece_len, std::sync::atomic::Ordering::Relaxed);

        // Notify others that we have a new piece and check if torrent is finished
        let _ = state.have_broadcast_tx.send(piece_index);
        state.new_pieces_notify.notify_waiters();
        state.on_piece_completed(piece_index)?;
        state.transmit_haves(piece_index);
    }

    info!("Piece {} successfully downloaded and verified from web seed", piece_index);

    Ok(())
}

/// Helper to write a chunk to storage by mapping it to file offsets
fn write_chunk_to_storage(
    storage: &crate::type_aliases::FileStorage,
    file_infos: &[crate::torrent_state::FileInfo],
    chunk_info: &ChunkInfo,
    data: &[u8],
    lengths: &Lengths,
) -> anyhow::Result<()> {
    let mut absolute_offset = lengths.chunk_absolute_offset(chunk_info);
    let mut data_offset = 0usize;
    let data_len = data.len();

    for (file_idx, file_info) in file_infos.iter().enumerate() {
        let file_len = file_info.len;

        // Skip files that are before this chunk
        if absolute_offset >= file_len {
            absolute_offset -= file_len;
            continue;
        }

        // Calculate how much to write in this file
        let remaining_in_chunk = data_len - data_offset;
        let remaining_in_file = (file_len - absolute_offset) as usize;
        let to_write = std::cmp::min(remaining_in_chunk, remaining_in_file);

        // Write to this file
        storage.pwrite_all(
            file_idx,
            absolute_offset,
            &data[data_offset..data_offset + to_write],
        )?;

        data_offset += to_write;

        // If we've written all the data, we're done
        if data_offset >= data_len {
            break;
        }

        // Move to the next file
        absolute_offset = 0;
    }

    Ok(())
}
