use uuid::Uuid;

use crate::{block::{errors::HeaderDeserialiseError, metadata::{Header, HEADER_SIZE}}, config::BLOCK_SIZE};

pub mod metadata;
pub mod errors;
pub(crate) mod sha3;

#[derive(PartialEq, Clone)]
pub struct Block {
    header: Header,
    body: [u8; BLOCK_SIZE - HEADER_SIZE]
}

impl Block {
    pub fn new(
        header: Header,
        body: [u8; BLOCK_SIZE - HEADER_SIZE]
    ) -> Self {
        Self {
            header,
            body
        }
    }

    pub fn deserialise_from_bytes(bytes: [u8; BLOCK_SIZE]) -> Result<Self, HeaderDeserialiseError> {
        let header = Header::deserialise_from_bytes(bytes[..HEADER_SIZE].try_into().map_err(|_| HeaderDeserialiseError::MalformedHeader)?)?;
        let body: [u8; BLOCK_SIZE - HEADER_SIZE] = bytes[HEADER_SIZE..].try_into().unwrap();

        Ok(Self::new(header, body))
    }

    pub fn serialise_to_bytes(&self) -> [u8; BLOCK_SIZE] {
        let mut bytes = [0u8; BLOCK_SIZE];

        bytes[..HEADER_SIZE].copy_from_slice(&self.header.serialise_to_bytes());
        bytes[HEADER_SIZE..].copy_from_slice(&self.body);

        bytes
    }

    pub fn body(&self) -> [u8; BLOCK_SIZE - HEADER_SIZE] {
        self.body.clone()
    }

    pub fn uuid(&self) -> Uuid {
        self.header.uuid()
    }

    pub fn replace_body(&mut self, new_body: [u8; BLOCK_SIZE - HEADER_SIZE]) {
        self.body = new_body;
        self.header.update_checksum(new_body);
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }
}
