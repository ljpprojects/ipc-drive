use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum ContiguousBlockError {
    /// This error is returned when more blocks cannot be created because the number of blocks would exceed the limit for the amount of blocks.
    TooManyBlocks,

    /// This error is returned when an I/O operation crosses block boundaries.
    /// This is only returned when a ContiguousBlocks has manually been configured to disallow the crossing of block boundaries.
    CrossesBlockBoundaries,

    /// This is similar to the TooManyBlocks error, but is returned to indicate that the maximum
    /// number of blocks is 1 and the I/O operation would need another block.
    NewBlockRequired,

    /// This is returned when a block read from the Store was corrupted, the ContiguousBlocks is configured to disallow the return of
    /// corrupted data (enabled by default), and after the configured amount of retries (3 by default) a non-corrupt block could not be read.
    DataCorrupted,

    /// This is similar to the DataCorrupted error, but is used to indicate that data was not valid, like an invalid UUID in the headers,
    /// or a body/header that was too short. You cannot, however, configure the ContiguousBlocks to allow invalid data.
    DataInvalid,

    /// This error is returned when the ContiguousBlocks was configured to disallow writes, but a write operation was attempted.
    CannotWrite,

    /// This error is returned when some other error occurs.
    Unknwon(Box<dyn Error>),
}

impl Display for ContiguousBlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for ContiguousBlockError {}
