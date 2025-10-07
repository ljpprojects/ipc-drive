use crate::{block::metadata::HEADER_SIZE, config::BLOCK_SIZE};

/// Returns the padded data and its original length
pub fn pad_data_to_block_size(data: &[u8]) -> ([u8; BLOCK_SIZE - HEADER_SIZE], u16) {
    let mut padded_data = [0u8; BLOCK_SIZE - HEADER_SIZE];
    let len = data.len().min(BLOCK_SIZE - HEADER_SIZE);

    padded_data[..len].copy_from_slice(&data[..len]);

    (padded_data, len as u16)
}
