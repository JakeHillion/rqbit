use std::sync::Arc;

use anyhow::{Context, bail};
use bytes::Bytes;
use librqbit_core::{
    lengths::{ChunkInfo, Lengths, ValidPieceIndex},
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};
use reqwest::Client;
use tracing::debug;

use super::WebSeedStates;

/// Downloads chunks from HTTP web seeds using Range requests
pub struct WebSeedDownloader {
    client: Arc<Client>,
    torrent_info: Arc<ValidatedTorrentMetaV1Info<buffers::ByteBufOwned>>,
    lengths: Lengths,
    web_seed_states: Arc<WebSeedStates>,
}

impl WebSeedDownloader {
    pub fn new(
        client: Arc<Client>,
        torrent_info: Arc<ValidatedTorrentMetaV1Info<buffers::ByteBufOwned>>,
        web_seed_states: Arc<WebSeedStates>,
    ) -> Self {
        let lengths = *torrent_info.lengths();
        Self {
            client,
            torrent_info,
            lengths,
            web_seed_states,
        }
    }

    /// Download a chunk from a web seed using HTTP Range request
    /// Handles chunks that may span multiple files
    pub async fn download_chunk(
        &self,
        web_seed_url: &str,
        chunk_info: ChunkInfo,
    ) -> anyhow::Result<Bytes> {
        let chunk_absolute_offset = self.lengths.chunk_absolute_offset(&chunk_info);
        let chunk_end = chunk_absolute_offset + chunk_info.size as u64;

        let mut result = Vec::with_capacity(chunk_info.size as usize);
        let mut current_offset = chunk_absolute_offset;

        // A chunk may span multiple files, so we iterate through all files that contain parts of this chunk
        for file_details in self.torrent_info.iter_file_details_ext() {
            let file_start = file_details.offset;
            let file_end = file_start + file_details.details.len;

            // Check if this file contains any part of the chunk
            if current_offset >= file_end || chunk_end <= file_start {
                continue; // This file doesn't overlap with the chunk
            }

            // Calculate which bytes we need from this file
            let read_start = if current_offset > file_start {
                current_offset - file_start
            } else {
                0
            };

            let read_end = std::cmp::min(chunk_end - file_start, file_details.details.len) - 1; // Inclusive

            // Construct URL for this file
            let url = self.construct_file_url(web_seed_url, &file_details.details.filename)?;

            debug!(
                "Downloading from {}: bytes {}-{} (chunk piece: {}, chunk: {})",
                url, read_start, read_end, chunk_info.piece_index, chunk_info.chunk_index
            );

            // Make HTTP Range request
            let response = self
                .client
                .get(&url)
                .header("Range", format!("bytes={}-{}", read_start, read_end))
                .send()
                .await
                .context("Failed to send HTTP request")?;

            let status = response.status();
            if !status.is_success() && status.as_u16() != 206 {
                bail!("HTTP request failed with status: {}", status);
            }

            let bytes = response
                .bytes()
                .await
                .context("Failed to read response body")?;

            result.extend_from_slice(&bytes);
            current_offset += bytes.len() as u64;

            // If we've collected all the bytes we need, stop
            if current_offset >= chunk_end {
                break;
            }
        }

        // Verify we got the expected number of bytes
        if result.len() != chunk_info.size as usize {
            bail!(
                "Expected {} bytes but got {}",
                chunk_info.size,
                result.len()
            );
        }

        // Record stats
        self.web_seed_states
            .record_bytes_downloaded(web_seed_url, result.len() as u64);
        self.web_seed_states.record_request_success(web_seed_url);

        Ok(Bytes::from(result))
    }

    /// Construct the URL for downloading a chunk and calculate the offset within the file
    /// Returns (url, file_offset) where file_offset is relative to the start of the specific file
    fn construct_url_and_offset_for_chunk(
        &self,
        base_url: &str,
        chunk_info: ChunkInfo,
    ) -> anyhow::Result<(String, u64)> {
        // Get the absolute offset of the chunk in the torrent
        let chunk_offset = self.lengths.chunk_absolute_offset(&chunk_info);

        // Find which file(s) this chunk belongs to
        // Iterate through files to find which one contains this offset
        for file_details in self.torrent_info.iter_file_details_ext() {
            let file_start = file_details.offset;
            let file_end = file_start + file_details.details.len;

            // Check if chunk starts in this file
            if chunk_offset >= file_start && chunk_offset < file_end {
                // Construct URL for this file
                let url = self.construct_file_url(base_url, &file_details.details.filename)?;
                // Calculate offset within this specific file
                let file_offset = chunk_offset - file_start;
                return Ok((url, file_offset));
            }
        }

        bail!("Could not find file for chunk offset {}", chunk_offset);
    }

    /// Construct the full URL for a file (BEP-19)
    /// Base URL + torrent_name (for multi-file) + file path components
    fn construct_file_url(
        &self,
        base_url: &str,
        filename: &librqbit_core::torrent_metainfo::FileIteratorName<buffers::ByteBufOwned>,
    ) -> anyhow::Result<String> {
        let mut url = base_url.to_string();

        // Ensure base URL ends with /
        if !url.ends_with('/') {
            url.push('/');
        }

        // For multi-file torrents, prepend the torrent name as the root directory
        // BEP-19 requires: base_url + torrent_name + "/" + file_path
        let mut path_components = Vec::new();

        if let Some(torrent_name) = self.torrent_info.name() {
            if !torrent_name.is_empty() {
                path_components.push(urlencoding::encode(&torrent_name).into_owned());
            }
        }

        // Add file path components, URL-encoding each part
        for component in filename.iter_components() {
            path_components.push(urlencoding::encode(&component).into_owned());
        }

        url.push_str(&path_components.join("/"));

        Ok(url)
    }

    /// Download a whole piece from a web seed
    /// This downloads all chunks of a piece and concatenates them
    pub async fn download_piece(
        &self,
        web_seed_url: &str,
        piece_index: ValidPieceIndex,
    ) -> anyhow::Result<Vec<u8>> {
        let piece_length = self.lengths.piece_length(piece_index);
        let mut piece_data = Vec::with_capacity(piece_length as usize);

        // Download all chunks for this piece
        for chunk_info in self.lengths.iter_chunk_infos(piece_index) {
            let chunk_bytes = self.download_chunk(web_seed_url, chunk_info).await?;
            piece_data.extend_from_slice(&chunk_bytes);
        }

        Ok(piece_data)
    }
}

/// Helper to determine if an HTTP status code indicates a permanent failure
pub fn is_permanent_failure(status: &reqwest::StatusCode) -> bool {
    matches!(
        status.as_u16(),
        404 | 403 | 410 | 451 // Not Found, Forbidden, Gone, Unavailable For Legal Reasons
    )
}

/// Helper to determine if an HTTP status code indicates a temporary failure
pub fn is_temporary_failure(status: &reqwest::StatusCode) -> bool {
    status.is_server_error() || status.as_u16() == 429 // 5xx or Too Many Requests
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_construction() {
        // Test that URL construction properly includes torrent name for multi-file torrents
        let base_url = "https://example.com/files/";
        let torrent_name = "my-torrent-hash";
        let file_path = vec!["subdir", "file.txt"];

        let mut components = Vec::new();
        components.push(urlencoding::encode(torrent_name).into_owned());
        for part in &file_path {
            components.push(urlencoding::encode(part).into_owned());
        }

        let url = format!("{}{}", base_url, components.join("/"));

        assert_eq!(url, "https://example.com/files/my-torrent-hash/subdir/file.txt");
    }

    #[test]
    fn test_url_construction_with_trailing_slash() {
        let base_url = "https://example.com/files";
        let torrent_name = "hash123";
        let file_path = "test.bin";

        let mut url = base_url.to_string();
        if !url.ends_with('/') {
            url.push('/');
        }
        url.push_str(&format!("{}/{}", torrent_name, file_path));

        assert_eq!(url, "https://example.com/files/hash123/test.bin");
    }

    #[test]
    fn test_url_encoding() {
        let base_url = "https://example.com/";
        let torrent_name = "my torrent";
        let file_path = "path with spaces/file name.txt";

        let parts: Vec<_> = std::iter::once(torrent_name)
            .chain(file_path.split('/'))
            .map(|p| urlencoding::encode(p).into_owned())
            .collect();

        let url = format!("{}{}", base_url, parts.join("/"));

        assert_eq!(url, "https://example.com/my%20torrent/path%20with%20spaces/file%20name.txt");
    }
}
