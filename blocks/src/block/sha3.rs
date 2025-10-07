use sha3::{
    Digest,
    Sha3_256,
};

use crate::{block::metadata::HEADER_SIZE, config::BLOCK_SIZE};

pub(crate) type Sha3Hash = [u8; 32];

pub(crate) fn generate_sha3_hash_for_block(block: &[u8; BLOCK_SIZE - HEADER_SIZE]) -> Sha3Hash {
    let mut hasher = Sha3_256::new();
    hasher.update(block);

    let raw_hash = hasher.finalize();

    let mut hash = [0u8; 32];
    hash.copy_from_slice(&raw_hash);

    hash
}
