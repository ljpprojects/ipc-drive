#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use chrono::{DateTime, Utc};
use uuid::{ContextV7, Timestamp, Uuid};

use crate::{block::{errors::HeaderDeserialiseError, sha3::{generate_sha3_hash_for_block, Sha3Hash}}, config::BLOCK_SIZE};

pub mod HeaderFlag {
    pub const IncludeDataHeaders: u8 = 0b0000_0001;
}

pub const HEADER_SIZE: usize = 64;

#[derive(PartialEq, Clone, Debug)]
pub struct Header {
    /// A checksum used to verify block integrity.
    checksum: Sha3Hash,

    /// A UUID (v7) used to identify blocks.
    uuid: Uuid,

    creation_time: DateTime<Utc>,

    /// How many times the block has been read.
    /// A read is when the block's contents are copied before sending it back tpo the slave process.
    read_count: u32,
    write_count: u32,

    /// How many times the block has been passed to a slave process.
    /// This should always be greater than or equal to the number of reads.
    pass_count: u32,

    /// An optional header to store the original length of the data.
    /// Its presence is dictated by the presence of the IncludeDataHeaders flag.
    original_data_len: Option<u16>,

    /// All applied flags.
    flags: Vec<u8>,
}

impl Header {
    pub fn new(
        body: &[u8; BLOCK_SIZE - HEADER_SIZE],
        read_count: u32,
        write_count: u32,
        pass_count: u32,
        original_data_len: Option<u16>,
        flags: Vec<u8>
    ) -> Self {
        let checksum = generate_sha3_hash_for_block(body);
        let uuid = Uuid::new_v7(Timestamp::now(ContextV7::new()));

        let (seconds, nseconds) = uuid.get_timestamp().unwrap().to_unix();

        Self {
            checksum,
            uuid,
            creation_time: DateTime::from_timestamp(seconds as i64, nseconds).unwrap(),
            read_count,
            write_count,
            pass_count,
            original_data_len,
            flags,
        }
    }

    pub fn new_with_uuid(
        body: &[u8; BLOCK_SIZE - HEADER_SIZE],
        uuid: Uuid,
        read_count: u32,
        write_count: u32,
        pass_count: u32,
        original_data_len: Option<u16>,
        flags: Vec<u8>
    ) -> Self {
        let checksum = generate_sha3_hash_for_block(body);

        let (seconds, nseconds) = uuid.get_timestamp().unwrap().to_unix();

        Self {
            checksum,
            uuid,
            creation_time: DateTime::from_timestamp(seconds as i64, nseconds).unwrap(),
            read_count,
            write_count,
            pass_count,
            original_data_len,
            flags,
        }
    }

    /// Serialises the header into bytes, which can then be appended to the start of the data.
    pub fn serialise_to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];

        // Add the checksum
        bytes[..32].copy_from_slice(&self.checksum);

        // Add the UUID
        bytes[32..48].copy_from_slice(&self.uuid.into_bytes());

        // Add the read, pass, and write counts
        bytes[48..52].copy_from_slice(&self.pass_count.to_be_bytes());
        bytes[52..56].copy_from_slice(&self.read_count.to_be_bytes());
        bytes[56..60].copy_from_slice(&self.write_count.to_be_bytes());

        // Add the flags
        let mut flag_byte = 0u8;

        for flag in self.flags.iter() {
            flag_byte |= flag
        }

        bytes[60] = flag_byte;

        // Add the original data length if it should be present
        if let Some(original_data_len) = self.original_data_len && self.flags.contains(&HeaderFlag::IncludeDataHeaders) {
            bytes[61..63].copy_from_slice(&original_data_len.to_be_bytes());
        }

        bytes
    }

    /// Serialises the header into bytes, which can then be appended to the start of the data.
    pub fn deserialise_from_bytes(bytes: &[u8; HEADER_SIZE]) -> Result<Self, HeaderDeserialiseError> {
        let checksum: Sha3Hash = bytes[..32].try_into().map_err(|_| HeaderDeserialiseError::MalformedHeader)?;
        let uuid = Uuid::from_bytes(bytes[32..48].try_into().map_err(|_| HeaderDeserialiseError::MalformedHeader)?);

        let pass_count = u32::from_be_bytes(bytes[48..52].try_into().map_err(|_| HeaderDeserialiseError::MalformedHeader)?);
        let read_count = u32::from_be_bytes(bytes[52..56].try_into().map_err(|_| HeaderDeserialiseError::MalformedHeader)?);
        let write_count = u32::from_be_bytes(bytes[56..60].try_into().map_err(|_| HeaderDeserialiseError::MalformedHeader)?);

        let flag_byte = bytes[60];
        let all_flags = [
            HeaderFlag::IncludeDataHeaders
        ];

        let mut enabled_flags = vec![];

        // Compare the flag_byte against all flags to see which ones apply
        for flag in all_flags {
            if flag_byte & flag == flag {
                enabled_flags.push(flag);
            }
        }

        // Check if the origjnal data length header is (or should be) present
        let original_data_len = if enabled_flags.contains(&HeaderFlag::IncludeDataHeaders) {
            Some(u16::from_be_bytes(bytes[61..63].try_into().map_err(|_| HeaderDeserialiseError::MalformedHeader)?))
        } else {
            None
        };

        let (seconds, nseconds) = uuid.get_timestamp().ok_or(HeaderDeserialiseError::MalformedHeader)?.to_unix();

        Ok(Self {
            checksum,
            uuid,
            creation_time: DateTime::from_timestamp(seconds as i64, nseconds).unwrap(),
            pass_count,
            read_count,
            write_count,
            original_data_len,
            flags: enabled_flags,
        })
    }

    pub fn checksum(&self) -> Sha3Hash {
        self.checksum.clone()
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid.clone()
    }

    pub fn creation_time(&self) -> DateTime<Utc> {
        self.creation_time.clone()
    }

    pub fn read_count(&self) -> u32 {
        self.read_count
    }

    pub fn write_count(&self) -> u32 {
        self.write_count
    }

    pub fn pass_count(&self) -> u32 {
        self.pass_count
    }

    pub fn original_data_len(&self) -> Option<u16> {
        self.original_data_len.clone()
    }

    pub fn flags(&self) -> Vec<u8> {
        self.flags().clone()
    }

    pub fn update_checksum(&mut self, new_body: [u8; BLOCK_SIZE - HEADER_SIZE]) {
        self.checksum = generate_sha3_hash_for_block(&new_body);
    }

    pub fn inc_read_count(&mut self, by: u32) {
        self.read_count += by;
    }

    pub fn inc_write_count(&mut self, by: u32) {
        self.write_count += by;
    }

    pub fn inc_pass_count(&mut self, by: u32) {
        self.pass_count += by;
    }

    pub fn swap_uuid(&mut self, to: Uuid) {
        self.uuid = to;
    }

    pub fn set_creation_time(&mut self, to: DateTime<Utc>) {
        self.creation_time = to;
    }
}
