use std::{collections::HashMap, error::Error, fmt::{Debug, Display}, sync::{mpsc::RecvTimeoutError, Arc}, time::Duration};

use smol::{channel::{self, RecvError, Sender}, future::FutureExt, lock::RwLock, Timer};
use uuid::Uuid;

use crate::{block::Block, config::{TIMEOUT_MS}};

#[derive(Clone, PartialEq)]
pub enum Operation {
    Read,

    /// The block only needs to have the checksum header, flags, and other headers associated with the flags.
    /// The block's headers should indicate that this block is fresh; it should be without any passes, reads, or writes.
    Replace(Block),
    Delete,
}

impl Debug for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read => write!(f, "Read"),
            Self::Replace(_) => write!(f, "Replace"),
            Self::Delete => write!(f, "Delete"),
        }
    }
}

impl Operation {
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        return match self {
            Self::Read => matches!(other, Self::Read),
            Self::Delete => matches!(other, Self::Delete),
            Self::Replace(c1) => match other {
                Self::Replace(c2) => c1.header().checksum() == c2.header().checksum(),
                _ => false
            }
        }
    }
}

pub struct IncompatibleOperationError(Operation, Operation);

impl Debug for IncompatibleOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Operations {:?} and {:?} are not compatible.", self.0, self.1)
    }
}

impl Display for IncompatibleOperationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for IncompatibleOperationError {}

#[derive(Clone)]
pub struct OperationCompletionData {
    pub block: Option<Block>
}

#[derive(Default)]
pub struct OperationsManager {
    /// Queued operations
    pub(crate) operation_queue: HashMap<Uuid, (Operation, Vec<Arc<Sender<OperationCompletionData>>>)>,
}

impl OperationsManager {
    pub fn new() -> Arc<RwLock<OperationsManager>> {
        Arc::new(RwLock::new(Self {
            operation_queue: HashMap::new(),
        }))
    }

    pub fn add_operation(&mut self, op: Operation, on: Uuid, sender: Arc<Sender<OperationCompletionData>>) -> Result<(), IncompatibleOperationError> {
        if let Some((old_op, senders)) = self.operation_queue.get_mut(&on) {
            // Ensure operations are compatible
            if !old_op.is_compatible_with(&op) {
                return Err(IncompatibleOperationError(old_op.clone(), op))
            }

            senders.push(sender);
        } else {
            self.operation_queue.insert(on.clone(), (op, vec![sender]));
        }

        Ok(())
    }

    pub fn remove_operations(&mut self, on: &Uuid) {
        self.operation_queue.remove(on);
    }

    pub async fn do_operation(op_manager: Arc<RwLock<OperationsManager>>, op: Operation, on: Uuid) -> Result<OperationCompletionData, Box<dyn Error + Send + Sync>> {
        let (tx, rx) = channel::bounded::<OperationCompletionData>(16);

        let tx = Arc::new(tx);

        op_manager.write().await.add_operation(op, on, tx).unwrap();

        rx.recv().or(async {
            Timer::after(Duration::from_millis(TIMEOUT_MS)).await;

            Err(RecvError) // Make some shit up
        }).await.map_err(|_| Box::new(RecvTimeoutError::Timeout) as Box<dyn Error + Send + Sync>)
    }

    pub fn take_operation_for(&mut self, block: &Uuid) -> Option<(Operation, Vec<Arc<Sender<OperationCompletionData>>>)> {
        self.operation_queue.remove(block)
    }

    pub async fn wait_for_operations(&self) {
        loop {
            if self.operation_queue.len() == 0 {
                break
            }
        }
    }
}
