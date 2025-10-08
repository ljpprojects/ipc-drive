/*pub mod io;
pub mod errors;
pub mod contiguous;*/

use event_listener::Event;
use smol::future::FutureExt;
use smol::lock::RwLock;
use smol::Timer;
use tracing::{error, info, warn};

use std::collections::HashSet;
use std::error::Error;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{fs, thread};

use blocks::block::Block;
use blocks::ipc::util::pad_data_to_block_size;
use blocks::config::{BLOCK_SIZE, TIMEOUT_MS};
use blocks::block::metadata::{Header, HeaderFlag, HEADER_SIZE};
use blocks::ipc::EMPTY_BLOCK_BODY;
use blocks::store::op::{Operation, OperationsManager};
use blocks::store::Store;

fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();

    smol::block_on(async {
        info!(
            srcfile = "blocks/target/release/master-proc",
            "Splitting source file into chunks..."
        );

        let data_chunks = include_bytes!("../blocks/target/release/master-proc").chunks(BLOCK_SIZE - HEADER_SIZE);
        let mut padded_chunks = data_chunks.map(|c| (c, c.len())).collect::<Vec<(&[u8], usize)>>();

        let chunk_count = padded_chunks.len();

        let Some((dat, _)) = padded_chunks.last_mut() else {
            unreachable!("There are no chunks.")
        };

        info!(
            chunk_count,
            "Padding last chunk to fit minumum block size..."
        );

        let padded_last = pad_data_to_block_size((*dat).try_into().unwrap()).0;

        *dat = &padded_last;

        info!(
            block_count = chunk_count,
            "Creating blocks..."
        );

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

        info!(
            block_count = chunk_count,
            "Adding blocks to store..."
        );

        for block in blocks.iter().cloned() {
            pids.insert(Store::add_block(store.clone(), block).await.map_err(|e| e as Box<dyn Error>)?);
        }

        let proc_count = pids.len();

        info!(
            process_count = proc_count,
            "Starting process event loops..."
        );

        Store::start_event_loops(store.clone(), op_manager.clone(), pids.clone());

        let delay_s = u64::from_str_radix(&*std::env::args().collect::<Vec<String>>().get(1).unwrap_or(&"30".to_string()), 10).unwrap_or(30);

        info!(
            time = delay_s,
            "Waiting..."
        );

        thread::sleep(Duration::from_secs(delay_s));

        let chunks = Arc::new(RwLock::new(vec![EMPTY_BLOCK_BODY.to_vec(); blocks.len()]));
        let progress = Arc::new(AtomicI32::new(0));
        let event_done = Arc::new(Event::new());

        for pid in pids.clone() {
            let proc_blocks = store.read().await.active_blocks().iter().flat_map(|(_, (blk, p))| {
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
            let event_done = event_done.clone();
            let progress = progress.clone();
            let pids = pids.clone();

            info!(
                "Reading blocks of proccess {pid}"
            );

            smol::spawn(async move {
                for block in proc_blocks.clone() {
                    // Check that the requested block exists
                    if !store.read().await.active_blocks().contains_key(&block.uuid()) {
                        warn!(
                            "Block {:?} is not present",
                            block.uuid()
                        );

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
                            error!(
                                "Could not perform get operation on block {:?}:\n{e}",
                                block.uuid()
                            );

                            store.write().await.remove_block(&block.uuid());

                            return
                        }
                    };

                    let blk = res.block.unwrap();
                    let index = blocks.iter().position(|b| b.uuid() == blk.uuid()).unwrap();

                    let _ = async {
                        chunks.write().await[index] = Vec::from(&blk.body()[..blk.header().original_data_len().map(|l| l as usize).unwrap_or(BLOCK_SIZE - HEADER_SIZE)]);
                    }.or(async {
                        Timer::after(Duration::from_millis(TIMEOUT_MS)).await;
                    }).await;
                }

                if progress.fetch_add(1, Ordering::Relaxed) + 1 == pids.len() as i32 {
                    event_done.notify(isize::MAX);
                }
            }).detach();
        }

        let _ = event_done.listen().await;

        info!(
            "Concatenating read data..."
        );

        let data = chunks.read().await.concat();

        info!(
            dist_file = "recv-master-bin",
            "Writing received data to file."
        );

        fs::write("./recv-master-bin", data)?;

        Ok(())
    })
}
