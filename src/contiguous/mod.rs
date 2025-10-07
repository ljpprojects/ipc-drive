use std::{collections::HashMap, range::Range, sync::Arc};

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{block::{metadata::{Header, HEADER_SIZE}, Block}, config::BLOCK_SIZE, ipc::EMPTY_BLOCK_BODY, store::{op::OperationsManager, Store}};

// mod io;
pub mod errors;

struct ContiguousBlocksBuilder {
    block_limit: u16,
    io_can_cross_block_boundaries: bool,
    allow_return_of_corrupt_data: bool,
    allow_writes: bool,
    initial_block_count: u16,

    store: Option<Arc<RwLock<Store>>>,
    op_manager: Option<Arc<RwLock<OperationsManager>>>,
}

impl ContiguousBlocksBuilder {
    fn new() -> Self {
        Self {
            block_limit: 128,
            io_can_cross_block_boundaries: true,
            allow_return_of_corrupt_data: false,
            allow_writes: true,
            initial_block_count: 1,
            store: None,
            op_manager: None
        }
    }

    pub fn block_limit(&mut self, lim: u16) -> &mut Self {
        self.block_limit = lim;

        self
    }

    pub fn no_cross_block_boundaries(&mut self) -> &mut Self {
        self.io_can_cross_block_boundaries = false;

        self
    }

    pub fn allow_return_of_corrupt_data(&mut self) -> &mut Self {
        self.allow_return_of_corrupt_data = true;

        self
    }

    pub fn readonly(&mut self) -> &mut Self {
        self.allow_writes = false;

        self
    }

    pub fn initial_block_count(&mut self, count: u16) -> &mut Self {
        self.initial_block_count = count;

        self
    }

    pub fn no_initial_blocks(&mut self) -> &mut Self {
        self.initial_block_count(0)
    }

    pub fn with_store(&mut self, store: Arc<RwLock<Store>>) -> &mut Self {
        self.store = Some(store);

        self
    }

    pub fn with_op_manager(&mut self, op_manager: Arc<RwLock<OperationsManager>>) -> &mut Self {
        self.op_manager = Some(op_manager);

        self
    }

    pub async fn build(mut self) -> ContiguousBlocks {
        let store = self.store.unwrap_or(Store::new());
        let op_manager = self.op_manager.unwrap_or(OperationsManager::new());

        let mut blocks = HashMap::new();

        // Add intial blocks
        for n in 0..self.initial_block_count {
            let block = Block::new(
                Header::new(
                    &EMPTY_BLOCK_BODY,
                    0,
                    0,
                    0,
                    None,
                    vec![],
                ),
                EMPTY_BLOCK_BODY
            );

            blocks.insert(block.uuid(), Range::from((n as u64 * (BLOCK_SIZE - HEADER_SIZE) as u64)..(n as u64 * (BLOCK_SIZE - HEADER_SIZE) as u64 + (BLOCK_SIZE - HEADER_SIZE) as u64)));

            Store::add_block(store.clone(), block).await.unwrap();
        }

        ContiguousBlocks {
            store,
            op_manager,
            blocks,
            block_limit: self.block_limit,
            io_can_cross_block_boundaries: self.io_can_cross_block_boundaries,
            allow_return_of_corrupt_data: self.allow_return_of_corrupt_data,
            allow_writes: self.allow_writes
        }
    }
}

/// Represents contiguous blocks linked together into one structure, under one common Store and OperationsManager.
///
/// New blocks will be created when a write is requested to an out of bounds area.
/// There is a hard limit of 128 blocks by default, but this can be changed using a ContiguousBlocksBuilder.
/// After this limit, OutOfBounds errors will be returned.
pub struct ContiguousBlocks {
    store: Arc<RwLock<Store>>,
    op_manager: Arc<RwLock<OperationsManager>>,

    /// Key: Block's UUID
    ///
    /// Value: Range of addresses it stores
    blocks: HashMap<Uuid, Range<u64>>, // Note: it would be confusing if blocks were stored here and not in sync with the ones in Store, so only keep their UUIDs.

    block_limit: u16,
    io_can_cross_block_boundaries: bool,
    allow_return_of_corrupt_data: bool,
    allow_writes: bool,
}

impl ContiguousBlocks {
    pub fn builder() -> ContiguousBlocksBuilder {
        ContiguousBlocksBuilder::new()
    }
}
