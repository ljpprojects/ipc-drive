pub mod util;

use std::error::Error;

use smol::{
    process::{
        ChildStdin,
        ChildStdout,
    },
    io::{
        AsyncReadExt,
        AsyncWriteExt
    }
};

use crate::{block::{metadata::HEADER_SIZE, Block}, config::BLOCK_SIZE};

pub const SLAVE_BIN_PATH: &str = "./slave-bin";

pub const EMPTY_BLOCK_BODY: [u8; BLOCK_SIZE - HEADER_SIZE] = [0u8; BLOCK_SIZE - HEADER_SIZE];

// Send a data block and wait for a response.
pub async fn send_block(block: &Block, to_stdin: &mut ChildStdin, to_stdout: &mut ChildStdout) -> Result<Block, Box<dyn Error + Send + Sync>> {
    // Write data
    to_stdin.write_all(&block.serialise_to_bytes()).await?;

    let mut buffer = vec![0u8; BLOCK_SIZE];
    to_stdout.read(&mut buffer).await?;

    Ok(Block::deserialise_from_bytes(buffer.try_into().unwrap())?)
}
