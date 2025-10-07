use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum HeaderDeserialiseError {
    /// This error is returned when the header is malformed.
    /// Usually, this is because of data corruption.
    MalformedHeader,
}

impl Display for HeaderDeserialiseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for HeaderDeserialiseError {}
