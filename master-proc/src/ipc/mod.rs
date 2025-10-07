pub mod util;

use std::{
    error::Error, io::{
        Read,
        Write
    }, process::{
        ChildStdin as StdChildStdin,
        ChildStdout as StdChildStdout,
    }
};

use tokio::{
    process::{
        ChildStdin as TokChildStdin,
        ChildStdout as TokChildStdout,
    },
    io::{
        AsyncReadExt,
        AsyncWriteExt
    }
};

use crate::{block::{metadata::HEADER_SIZE, Block}, config::BLOCK_SIZE};

pub const SLAVE_BIN_PATH: &str = "../slave-proc/target/debug/slave-proc";

pub const EMPTY_BLOCK_BODY: [u8; BLOCK_SIZE - HEADER_SIZE] = [0u8; BLOCK_SIZE - HEADER_SIZE];

// Send a data block and wait for a response, blocking the thread
pub fn send_block_blocking(block: Block, to_stdin: &mut StdChildStdin, to_stdout: &mut StdChildStdout) -> Result<Block, Box<dyn Error + Send + Sync>> {
    // Write data
    to_stdin.write_all(&block.serialise_to_bytes())?;

    let mut buffer = vec![0u8; BLOCK_SIZE];
    to_stdout.read(&mut buffer)?;

    Ok(Block::deserialise_from_bytes(buffer.try_into().unwrap())?)
}

// Send a data block and wait for a response.
pub async fn send_block(block: Block, to_stdin: &mut TokChildStdin, to_stdout: &mut TokChildStdout) -> Result<Block, Box<dyn Error + Send + Sync>> {
    // Write data
    to_stdin.write_all(&block.serialise_to_bytes()).await?;

    let mut buffer = vec![0u8; BLOCK_SIZE];
    to_stdout.read(&mut buffer).await?;

    Ok(Block::deserialise_from_bytes(buffer.try_into().unwrap())?)
}
