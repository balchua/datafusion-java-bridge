//! Streaming query execution: pulls batches out of DataFusion and hands them to Java
//! one at a time, with cancellation support.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

use crate::error::{EngineError, ErrorKind, Result};

/// Capacity of the channel between the producer task and Java. Small on purpose:
/// bounds memory in flight and keeps producer/consumer paces coupled.
const CHANNEL_CAPACITY: usize = 8;

/// Shared state of one running query. Java holds it via the engine registry
/// (`resultNextBatch(resultId, ...)`) until `resultClose` drops it.
pub struct QueryHandle {
    pub schema: SchemaRef,
    rx: Mutex<mpsc::Receiver<Result<RecordBatch>>>,
    cancelled: AtomicBool,
    finished: AtomicBool,
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Wall-clock nanoseconds the producer spent producing all batches (0 until done).
    pub produce_nanos: AtomicU64,
    pub produced_rows: AtomicU64,
}

impl QueryHandle {
    /// Spawns the producer task on the engine's runtime. Planning must already be done;
    /// this only starts execution.
    pub fn start(
        rt: &tokio::runtime::Handle,
        schema: SchemaRef,
        plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        task_ctx: Arc<datafusion::execution::TaskContext>,
    ) -> Arc<QueryHandle> {
        let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(CHANNEL_CAPACITY);
        let handle = Arc::new(QueryHandle {
            schema,
            rx: Mutex::new(rx),
            cancelled: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            join: Mutex::new(None),
            produce_nanos: AtomicU64::new(0),
            produced_rows: AtomicU64::new(0),
        });

        let weak = Arc::downgrade(&handle);
        let join = rt.spawn(async move {
            let started = std::time::Instant::now();
            match datafusion::physical_plan::execute_stream(plan, task_ctx) {
                Ok(mut stream) => {
                    while let Some(next) = stream.next().await {
                        match next {
                            Ok(batch) => {
                                let producer_gone = weak.upgrade().is_none();
                                if producer_gone {
                                    break; // Java dropped the result: stop producing
                                }
                                if tx.send(Ok(batch)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(Err(e.into())).await;
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e.into())).await;
                }
            }
            if let Some(inner) = weak.upgrade() {
                inner.produce_nanos.store(
                    started.elapsed().as_nanos() as u64,
                    Ordering::Release,
                );
                inner.finished.store(true, Ordering::Release);
            }
            drop(tx); // close the channel -> Java sees end-of-stream
        });
        *handle.join.lock().unwrap() = Some(join);
        handle
    }

    /// Blocking receive of the next batch. Returns `None` at end of stream.
    /// Must be called from a non-async (JNI) thread.
    pub fn next(&self) -> Result<Option<RecordBatch>> {
        let mut rx = self.rx.lock().unwrap();
        match rx.blocking_recv() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => {
                // Drain remaining items so the producer never blocks on a dead consumer.
                while let Ok(_) = rx.try_recv() {}
                Err(e)
            }
            None => {
                if self.cancelled.load(Ordering::SeqCst) {
                    Err(EngineError::new(
                        ErrorKind::Cancelled,
                        "query was cancelled",
                    ))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Aborts the producer task. The next `next()` returns `QueryCancelledException`
    /// (or end-of-stream if it had already finished).
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.lock().unwrap().take() {
            join.abort();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}

impl Drop for QueryHandle {
    fn drop(&mut self) {
        self.cancel();
    }
}