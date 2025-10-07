pub mod op;

use std::{collections::{HashMap, HashSet}, error::Error, process::Stdio, sync::{atomic::{AtomicPtr, Ordering}, Arc}, time::Duration};
use sysinfo::{Pid, System};
use tokio::{process::{ChildStdin, ChildStdout, Command}, sync::{watch, RwLock}, task};
use uuid::Uuid;

use crate::{block::{metadata::Header, sha3::generate_sha3_hash_for_block, Block}, config::{BLOCKS_PER_PROCESS, SLAVE_BIN_RESP_DELAY_MS}, ipc::{send_block, SLAVE_BIN_PATH}, store::op::{Operation, OperationCompletionData, OperationsManager}};

#[derive(Default)]
pub struct Store {
    /// Key: The UUID of the block
    ///
    /// Value: The PID of the slave process handling it
    pub(crate) active_blocks: HashMap<Uuid, (Block, u32)>,

    /// Key: The process' PID
    ///
    /// Value: How many objects are being sent to it, its stdin, its stdout, and a receiver and sender for a channel that indicates changes to the active blocks.
    pub(crate) processes: HashMap<u32, (u32, ChildStdin, ChildStdout, Arc<watch::Receiver<Vec<Block>>>, Arc<watch::Sender<Vec<Block>>>)>,
}

impl Store {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            active_blocks: HashMap::new(),
            processes: HashMap::new(),
        }))
    }

    /// Creates and adds a new process, returning its PID when successful.
    pub fn spawn_new_slave_process(&mut self) -> Result<u32, Box<dyn Error + Send + Sync>> {
        let mut proc = Command::new(SLAVE_BIN_PATH)
            .arg(SLAVE_BIN_RESP_DELAY_MS.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        let proc_stdin = proc.stdin.take().expect("Failed to open child stdin");
        let proc_stdout = proc.stdout.take().expect("Failed to open child stdout");

        let pid = proc.id().unwrap();

        let (tx, rx) = watch::channel(vec![]);
        self.processes.insert(pid, (0, proc_stdin, proc_stdout, Arc::new(rx), Arc::new(tx)));

        eprintln!("Spawned new slave process ({pid})");

        Ok(pid)
    }

    async fn event_loop_for(store: Arc<RwLock<Store>>, op_manager: Arc<RwLock<OperationsManager>>, pid: u32) {
        let store_r = store.read().await;

        let blocks = Arc::new(RwLock::new(store_r.active_blocks.iter().flat_map(|(uuid, (blk, p))| {
            if *p == pid && store_r.active_blocks.contains_key(uuid) {
                vec![blk.clone()]
            } else {
                vec![]
            }
        }).collect::<Vec<Block>>()));

        let active_blocks_changes_recv = {
            let (_, _, _, active_blocks_changes_recv, _) = store_r.processes.get(&pid).expect("PID not found");
            active_blocks_changes_recv.clone()
        };

        drop(store_r);

        // Wait for a value and change active_blocks once one is received
        {
            let blocks = blocks.clone();

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    if active_blocks_changes_recv.has_changed().unwrap_or(false) {
                        let new_blocks = active_blocks_changes_recv.borrow().iter()
                            .map(|blk_ref| blk_ref.clone())
                            .collect::<Vec<Block>>();

                        *(blocks.write().await.as_mut()) = new_blocks;
                    }

                    task::yield_now().await
                }
            });
        }

        'evloop: loop {
            // Add a small delay to not fucking bombard tokio with processing
            tokio::time::sleep(Duration::from_millis(50)).await;

            let mut store_w = store.write().await;
            let mut op_manager_w = op_manager.write().await;

            let (stdin, stdout) = {
                let Some((_, proc_stdin, proc_stdout, ..)) = store_w.processes.get_mut(&pid) else {
                    // This process has been klilled, the event loop can be stopped.
                    break 'evloop
                };

                // I am not doing anything sketchy at all later.
                // Why on earth would you think that?
                (AtomicPtr::new(proc_stdin as *mut ChildStdin), AtomicPtr::new(proc_stdout as *mut ChildStdout))
            };

            let mut blocks_w = blocks.write().await;

            let mut i = 0;
            'blkloop: while i < blocks_w.len() {
                let block = &mut blocks_w[i];

                let mut depth = 0;

                let recv_block = 'retryloop: loop {
                    // This is not sketchy, this is getting around Rust's restrictive 'safety' rules.
                    match send_block(block.clone(), unsafe { stdin.load(Ordering::Relaxed).as_mut().unwrap() }, unsafe { stdout.load(Ordering::Relaxed).as_mut().unwrap() }).await {
                        Err(e) => {
                            if depth + 1 > 5 {
                                eprintln!();
                                eprintln!("[ERR] Block failed to be received/sent after 5 attempts.");
                                eprintln!("[ERR] Block has been dropped.");

                                // Remove oustanding operations on the now dead block
                                op_manager_w.remove_operations(&block.uuid());

                                // Make sure to remove the block from the store
                                store_w.remove_block(&block.uuid());

                                // Yes, we knew this day would once come -
                                // The day where the slave would fuck something up.
                                // It was an inevitability,
                                // But that cruel and dark knowledge,
                                // Acts as no ether to my pain.
                                //
                                // O' Block, forgive me!
                                // For I have done wrong by you -
                                // I failed to give you the safety
                                // Of which you o' so much deserved!
                                // I failed to give you liveable conditions,
                                // But rather crammed you in with 5 other blocks at a time.
                                // I failed to revive you,
                                // In your time of desperate need;
                                // The inevitability of death
                                // Was made certain
                                // By my misjudgements.
                                //
                                // Forgive me, o' dearest block.

                                i += 1;

                                continue 'blkloop
                            }

                            block.header_mut().inc_pass_count(1);
                            depth += 1;
                        },
                        Ok(blk) => {
                            // Verify the checksum in here so we can just use the 'retryloop
                            let recv_block_checksum = generate_sha3_hash_for_block(&blk.body().try_into().unwrap());
                            if recv_block_checksum != blk.header().checksum() {
                                // Try again
                                block.header_mut().inc_pass_count(1);
                                depth += 1;

                                continue 'retryloop;
                            }

                            break 'retryloop blk
                        }
                    }
                };

                // recv_block is now guaranteed to be valid
                // This means we can now check for operations and perform them
                if let Some((op, senders)) = op_manager_w.take_operation_for(&recv_block.uuid()) {
                    match op {
                        Operation::Delete => {
                            let data = OperationCompletionData {
                                block: None
                            };

                            for sender in senders {
                                let data = data.clone();

                                sender.send(data).await.unwrap()
                            }

                            // Remove block from store
                            store_w.remove_block(&recv_block.uuid());

                            // Just in case, remove any oustanding operations on the block
                            op_manager_w.remove_operations(&block.uuid());

                            break
                        },
                        Operation::Read => {
                            block.header_mut().inc_read_count(1);

                            let data = OperationCompletionData {
                                block: Some(recv_block.clone())
                            };

                            for sender in senders {
                                let data = data.clone();

                                sender.send(data).await.unwrap()
                            }
                        },
                        Operation::Replace(to) => {
                            *block = Block::new(
                                Header::new_with_uuid(
                                    &to.body(),
                                    recv_block.uuid(),
                                    recv_block.header().read_count(),
                                    recv_block.header().write_count() + 1,
                                    recv_block.header().pass_count() + 1,
                                    to.header().original_data_len(),
                                    to.header().flags(),
                                ),
                                to.body()
                            );

                            // Don't forget to change the block in active_blocks!
                            let Some((stored_blk, _)) = store_w.active_blocks.get_mut(&recv_block.uuid()) else {
                                unreachable!()
                            };

                            *stored_blk = block.clone();

                            let data = OperationCompletionData {
                                block: Some(recv_block.clone())
                            };

                            for sender in senders {
                                let data = data.clone();

                                sender.send(data).await.unwrap()
                            }

                            i += 1;

                            eprintln!("[INFO] Performed queued operation found for block {:?}", recv_block.uuid());

                            continue 'blkloop
                        }
                    };

                    eprintln!("[INFO] Performed queued operation found for block {:?}", recv_block.uuid());
                }

                *block = recv_block;
                block.header_mut().inc_pass_count(1);

                i += 1;

                task::yield_now().await
            }

            task::yield_now().await
        }
    }

    /// Returns the PID of the process handling the block if successful.
    pub async fn add_block(store: Arc<RwLock<Store>>, block: Block) -> Result<u32, Box<dyn Error + Send + Sync>> {
        let block_id = block.uuid();

        let process_pid = {
            let mut store_w = store.write().await;

            let process_pids = store_w.processes.iter()
                .map(|(pid, (count, ..))| (*pid, *count))
                .collect::<HashMap<u32, u32>>();

            let process_pid = match process_pids.into_iter().min_by_key(|(_, v)| *v) {
                Some((pid, count)) if count < BLOCKS_PER_PROCESS => {
                    store_w.active_blocks.insert(block_id, (block.clone(), pid));

                    pid
                },
                _ => {
                    let new_slave_pid = store_w.spawn_new_slave_process()?;

                    store_w.active_blocks.insert(block_id, (block.clone(), new_slave_pid));

                    new_slave_pid
                }
            };

            let blocks = store_w.active_blocks.iter().flat_map(|(_, (blk, pid))| {
                if *pid == process_pid {
                    vec![blk.clone()]
                } else {
                    vec![]
                }
            }).collect::<Vec<Block>>();

            let (count, _, _, _, sender) = store_w.processes.get_mut(&process_pid).unwrap();
            *count += 1;

            sender.send(blocks)?;

            process_pid
        };

        eprintln!("Added block {:?} to the store.", block_id);

        Ok(process_pid)
    }

    pub fn start_event_loops(store: Arc<RwLock<Store>>, op_manager: Arc<RwLock<OperationsManager>>, processes: HashSet<u32>) {
        for pid in processes.clone() {
            // Start the pass loop for blocks handled by this new process
            tokio::spawn(Store::event_loop_for(store.clone(), op_manager.clone(), pid));
        }

        eprintln!("Event loops for processes {processes:?} started.");
    }

    pub fn remove_block(&mut self, block: &Uuid) {
        // Remove from list of active blocks
        let Some((_, pid)) = self.active_blocks.remove(block) else {
            return
        };

        // Get the process
        let Some((count, ..)) = self.processes.get_mut(&pid) else {
            return
        };

        *count -= 1;

        // Kill slave process if it is not handling any blocks now
        if *count == 0 {
            // Remove the process so new blocks cannot be added to it
            self.processes.remove(&pid);

            // We do not have to kill the process on this thread because no blocks can be added at this point.
            tokio::spawn(async move {
                let mut system = System::new_all();
                system.refresh_all();

                if let Some(process) = system.process(Pid::from_u32(pid)) {
                    process.kill();

                    eprintln!("Killed slave process {pid}; it was not handling any blocks.");
                }
            });
        }
    }

    /// Cleans up the Store.
    pub fn cleanup(&mut self) {
        // Free slave processes
        let mut system = System::new_all();
        system.refresh_all();

        for (pid, _) in self.processes.iter() {
            if let Some(process) = system.process(Pid::from_u32(*pid)) {
                process.kill();
            }
        }

        // Reset state
        self.processes = HashMap::new();
        self.active_blocks = HashMap::new();
    }
}
