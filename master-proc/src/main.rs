#![feature(new_range_api)]

pub mod ipc;
pub mod block;
pub mod store;
mod device;
mod config;

use tokio::runtime;
use tokio::sync::{Notify, RwLock};
use tokio::time::timeout;

use std::collections::HashSet;
use std::error::Error;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use crate::block::Block;
use crate::ipc::util::pad_data_to_block_size;
use crate::config::{BLOCK_SIZE, TIMEOUT_MS};
use crate::block::metadata::{Header, HeaderFlag, HEADER_SIZE};
use crate::ipc::EMPTY_BLOCK_BODY;
use crate::store::op::{Operation, OperationsManager};
use crate::store::Store;

fn main() -> Result<(), Box<dyn Error>> {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(10)
        .thread_name_fn(|| {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);

            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);

            format!("tokio-worker-{}", id)
        })
        .build()
        .unwrap();

    let _guard = rt.enter();

    console_subscriber::init();

    rt.block_on(async {
        let data_chunks = include_bytes!("../target/debug/master-proc").chunks(BLOCK_SIZE - HEADER_SIZE);
        let mut padded_chunks = data_chunks.map(|c| (c, c.len())).collect::<Vec<(&[u8], usize)>>();

        let Some((dat, _)) = padded_chunks.last_mut() else {
            unreachable!()
        };

        let padded_last = pad_data_to_block_size((*dat).try_into().unwrap()).0;

        *dat = &padded_last;

        let blocks = padded_chunks.iter().map(|(chunk, len)| Block::new(
            Header::new(
                &(*chunk).try_into().unwrap(),
                0,
                0,
                0,
                Some(*len as u16),
                vec![HeaderFlag::IncludeDataHeaders]
            ),
            (*chunk).try_into().unwrap()
        )).collect::<Vec<Block>>();

        let store = Store::new();
        let op_manager = OperationsManager::new();
        let mut pids = HashSet::new();

        for block in blocks.iter().cloned() {
            pids.insert(Store::add_block(store.clone(), block).await.map_err(|e| e as Box<dyn Error>)?);
        }

        Store::start_event_loops(store.clone(), op_manager.clone(), pids.clone());

        let delay_s = u64::from_str_radix(&*std::env::args().collect::<Vec<String>>().get(1).unwrap_or(&"30".to_string()), 10).unwrap_or(30);
        thread::sleep(Duration::from_secs(delay_s));

        let chunks = Arc::new(RwLock::new(vec![EMPTY_BLOCK_BODY.to_vec(); blocks.len()]));
        let progress = Arc::new(AtomicI32::new(0));
        let notify_done = Arc::new(Notify::new());

        for pid in pids.clone() {
            let proc_blocks = store.read().await.active_blocks.iter().flat_map(|(_, (blk, p))| {
                if *p == pid {
                    vec![blk.clone()]
                } else {
                    vec![]
                }
            }).collect::<Vec<Block>>();

            let store = store.clone();
            let chunks = chunks.clone();
            let op_manager = op_manager.clone();
            let blocks = blocks.clone();
            let notify_done = notify_done.clone();
            let progress = progress.clone();
            let pids = pids.clone();

            tokio::spawn(async move {
                for block in proc_blocks {
                    // Check that the requested block exists
                    if !store.read().await.active_blocks.contains_key(&block.uuid()) {
                        eprintln!("[ERR] Block {:?} was deleted or is not present.", block.uuid());

                        store.write().await.remove_block(&block.uuid());

                        return
                    }

                    // Abstracted callback hell
                    let res = match OperationsManager::do_operation(op_manager.clone(), Operation::Read, block.uuid()).await {
                        Ok(r) => {
                            store.write().await.remove_block(&block.uuid());

                            r
                        },
                        Err(e) => {
                            eprintln!("[ERR] Could not get block {:?}: {e:?}", block.uuid());

                            store.write().await.remove_block(&block.uuid());

                            return
                        }
                    };

                    let blk = res.block.unwrap();
                    let index = blocks.iter().position(|b| b.uuid() == blk.uuid()).unwrap();

                    let _ = timeout(Duration::from_millis(TIMEOUT_MS), async {
                        chunks.write().await[index] = Vec::from(&blk.body()[..blk.header().original_data_len().map(|l| l as usize).unwrap_or(BLOCK_SIZE - HEADER_SIZE)]);
                    }).await;

                    if progress.fetch_add(1, Ordering::Relaxed) + 1 == pids.len() as i32 {
                        notify_done.notify_waiters();
                    }
                }
            });
        }

        notify_done.notified().await;
        let data = chunks.read().await.concat();

        fs::write("./src/recv-master-bin", data)?;

        Ok(())
    })
}
