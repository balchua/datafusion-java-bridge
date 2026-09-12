//! The engine: owns the tokio runtime, the DataFusion SessionContext, staged Arrow
//! tables, live queries and the object-store registry. One engine == one payment job.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::{SQLOptions, SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::datasource::MemTable;
use datafusion::prelude::ParquetReadOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use url::Url;

use crate::arrow_bridge;
use crate::error::{EngineError, ErrorKind, Result};
use crate::object_store::{ObjectStoreRegistry, S3Config};
use crate::parquet;
use crate::query::QueryHandle;
use crate::sql_guard;

/// Captured at compile time by build.rs.
pub const RUSTC_VERSION: &str = match option_env!("DFE_RUSTC_VERSION") {
    Some(v) => v,
    None => "unknown",
};

fn default_true() -> bool {
    true
}

/// Configuration passed as JSON from Java. Defaults are applied in `EngineConfig::load`.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default)]
pub struct EngineConfig {
    /// DataFusion memory pool limit (spillable operators only). 0 = unlimited.
    /// NOT a process memory limit — see docs/memory.md.
    pub memory_limit_bytes: u64,
    /// Tokio worker threads; also used as DataFusion target_partitions.
    pub parallelism: usize,
    /// Scratch directory for spilling and Parquet staging.
    pub scratch_dir: Option<String>,
    /// Parquet compression: snappy (default) | zstd | uncompressed.
    pub parquet_compression: Option<String>,
    /// S3-compatible storage configuration; required only when s3:// URLs are used.
    pub s3: Option<S3Config>,
    /// Ingest mode. `true` (default): zero-copy — Rust wraps the Java buffers via the
    /// C Data Interface; they stay alive until the table is dropped, and Java must not
    /// modify/reuse them meanwhile. `false`: deep-copy — Java Arrow buffers are copied
    /// into Rust-owned memory, so Java may free/reuse them immediately.
    #[serde(default = "default_true")]
    pub zero_copy: bool,
}

pub struct Engine {
    config: EngineConfig,
    rt: tokio::runtime::Runtime,
    ctx: SessionContext,
    staged: Mutex<HashMap<u64, StagedTable>>,
    queries: Mutex<HashMap<u64, Arc<QueryHandle>>>,
    object_stores: Mutex<ObjectStoreRegistry>,
    next_id: AtomicU64,
    /// Number of native calls currently inside this engine. `engineClose` waits for
    /// this to reach 0 after cancelling queries, so an in-flight `result_next` on a
    /// foreign thread can never touch a freed engine.
    pub active_calls: AtomicU64,
}

/// RAII guard for `Engine::active_calls`.
pub struct ActiveCall(*const Engine);

impl ActiveCall {
    /// Only `ffi::engine_from` may create guards (it increments under the registry lock).
    pub(crate) fn new(engine: &Engine) -> Self {
        Self(engine as *const Engine)
    }
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        // SAFETY: the engine is kept alive by `active_calls > 0`: `engineClose` removes
        // the handle from the registry, cancels queries and waits for this counter to
        // reach 0 BEFORE freeing the Box, and every holder holds the guard until after
        // its last engine access.
        unsafe { (*self.0).active_calls.fetch_sub(1, Ordering::SeqCst) };
    }
}

struct StagedTable {
    schema: SchemaRef,
    batches: Vec<datafusion::arrow::array::RecordBatch>,
}

fn select_only_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

impl Engine {
    /// Builds a version string for benchmark metadata.
    pub fn version() -> String {
        format!(
            "rust={} datafusion={} arrow={}",
            RUSTC_VERSION,
            datafusion::DATAFUSION_VERSION,
            datafusion::arrow::ARROW_VERSION
        )
    }

    /// Cancels all running queries. Called by `engineClose` before it waits for
    /// `active_calls` to drain.
    pub fn cancel_all_queries(&self) {
        for (_, handle) in self.queries.lock().unwrap().drain() {
            handle.cancel();
        }
    }

    /// Blocks until all in-flight native calls returned, or the deadline expires.
    /// Returns `true` if the engine is idle (safe to free), `false` if a call is still
    /// running (the caller must NOT free the engine — leaking is safer than a UAF).
    ///
    /// Called with the handle already removed from the registry, so no new calls can
    /// enter and `active_calls` cannot increase.
    pub fn wait_idle(&self) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while self.active_calls.load(Ordering::SeqCst) > 0 {
            if std::time::Instant::now() > deadline {
                // Do not block forever on a stuck call; the caller will leak the engine
                // rather than drop it, avoiding a use-after-free in that call.
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        true
    }

    pub fn create(config_json: &str) -> Result<Engine> {
        let config: EngineConfig = if config_json.trim().is_empty() {
            serde_json::from_str("{}")?
        } else {
            serde_json::from_str(config_json)?
        };

        let parallelism = if config.parallelism == 0 {
            std::thread::available_parallelism()
                .map(|n| n.get().min(4))
                .unwrap_or(4)
        } else {
            config.parallelism
        };

        let scratch_dir = config
            .scratch_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().to_string_lossy().to_string());

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(parallelism)
            .enable_all()
            .thread_name("df-engine")
            .build()?;

        let mut rt_builder = RuntimeEnvBuilder::new().with_temp_file_path(&scratch_dir);
        if config.memory_limit_bytes > 0 {
            rt_builder = rt_builder.with_memory_limit(config.memory_limit_bytes as usize, 1.0);
        }
        let runtime_env = rt_builder
            .build_arc()
            .map_err(|e| EngineError::new(ErrorKind::Resource, format!("runtime env: {e}")))?;

        let session_config = SessionConfig::new()
            .with_information_schema(true)
            .with_target_partitions(parallelism);

        let ctx = SessionContext::new_with_config_rt(session_config, runtime_env);
        let s3 = config.s3.clone();

        Ok(Engine {
            config,
            rt,
            ctx,
            staged: Mutex::new(HashMap::new()),
            queries: Mutex::new(HashMap::new()),
            object_stores: Mutex::new(ObjectStoreRegistry::new(s3)),
            next_id: AtomicU64::new(1),
            active_calls: AtomicU64::new(0),
        })
    }

    /// Imports a record batch exported by Java and stages it under an internal id.
    /// Java may free its vectors as soon as this call returns.
    pub fn stage_arrow(&self, table: &str, schema_ptr: usize, array_ptrs: &[usize]) -> Result<u64> {
        if table.is_empty() {
            return Err(EngineError::new(ErrorKind::SchemaMismatch, "empty table name"));
        }
        let (schema, batch) =
            arrow_bridge::import_batch(schema_ptr, array_ptrs, !self.config.zero_copy)?;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.staged.lock().unwrap().insert(
            id,
            StagedTable {
                schema: Arc::new(schema),
                batches: vec![batch],
            },
        );
        Ok(id)
    }

    /// Registers a staged table in DataFusion as an in-memory MemTable.
    pub fn commit_table(&self, table: &str, staged_id: u64) -> Result<()> {
        let staged = self
            .staged
            .lock()
            .unwrap()
            .remove(&staged_id)
            .ok_or_else(|| {
                EngineError::new(
                    ErrorKind::Internal,
                    format!("unknown staged table id {staged_id}"),
                )
            })?;
        let mem = MemTable::try_new(staged.schema, vec![staged.batches])
            .map_err(|e| EngineError::new(ErrorKind::SchemaMismatch, format!("mem table: {e}")))?;
        self.ctx
            .register_table(table, Arc::new(mem))
            .map_err(|e| EngineError::new(ErrorKind::SchemaMismatch, format!("registration failed: {e}")))?;
        Ok(())
    }

    /// Registers a Parquet-backed table. Parquet decoding stays entirely in DataFusion.
    pub fn register_parquet(&self, table: &str, location: &str) -> Result<()> {
        self.ensure_object_store(location)?;
        self.rt.block_on(async {
            self.ctx
                .register_parquet(table, location, ParquetReadOptions::default())
                .await
                .map_err(Into::into)
        })
    }

    /// Registers/caches the object store for the given URL if it is not local.
    fn ensure_object_store(&self, location: &str) -> Result<()> {
        let is_url = location.contains("://");
        if !is_url {
            return Ok(());
        }
        let url = Url::parse(location)
            .map_err(|e| EngineError::new(ErrorKind::ObjectStore, format!("bad location: {e}")))?;
        if url.scheme() == "file" {
            return Ok(());
        }
        let resolved = self
            .object_stores
            .lock()
            .unwrap()
            .store_for(&url)?;
        if let Some((store_url, store)) = resolved {
            self.ctx.register_object_store(&store_url, store);
        }
        Ok(())
    }

    fn resolve_store(&self, destination: &str) -> Result<Option<Arc<dyn object_store::ObjectStore>>> {
        if !destination.contains("://") || destination.starts_with("file://") {
            return Ok(None);
        }
        let url = Url::parse(destination)
            .map_err(|e| EngineError::new(ErrorKind::ObjectStore, format!("bad destination: {e}")))?;
        Ok(self
            .object_stores
            .lock()
            .unwrap()
            .store_for(&url)?
            .map(|(_, store)| store))
    }

    /// Validates SQL, plans the query (parse -> logical -> optimize -> physical) and
    /// starts execution on the runtime. Returns the query id.
    pub fn query(&self, sql: &str) -> Result<u64> {
        sql_guard::validate_select_only(sql)?;
        let (schema, plan) = self.plan(sql)?;
        let qid = self.next_id.fetch_add(1, Ordering::SeqCst);
        let handle = QueryHandle::start(self.rt.handle(), schema, plan, self.ctx.state().task_ctx());
        self.queries.lock().unwrap().insert(qid, handle);
        Ok(qid)
    }

    /// Blocking planning phase. Returns (result schema, physical plan).
    fn plan(&self, sql: &str) -> Result<(SchemaRef, Arc<dyn datafusion::physical_plan::ExecutionPlan>)> {
        self.rt.block_on(async {
            let state = self.ctx.state();
            let logical = state.create_logical_plan(sql).await?;
            select_only_options().verify_plan(&logical)?;
            let physical = state.create_physical_plan(&logical).await?;
            Ok((physical.schema(), physical))
        })
    }

    pub fn result_schema(&self, qid: u64, out_schema_ptr: usize) -> Result<()> {
        let handle = self.get_query(qid)?;
        arrow_bridge::export_schema(&handle.schema, out_schema_ptr)
    }

    /// Receives the next result batch and exports its columns into Java-provided
    /// ArrowArray structs. Returns `Some(row_count)` or `None` at end of stream.
    pub fn result_next(&self, qid: u64, out_array_ptrs: &[usize]) -> Result<Option<usize>> {
        let handle = self.get_query(qid)?;
        match handle.next()? {
            Some(batch) => {
                let columns = batch.columns();
                if out_array_ptrs.len() != columns.len() {
                    return Err(EngineError::new(
                        ErrorKind::ArrowBridge,
                        format!(
                            "column count mismatch: plan has {} columns, Java provided {} structs",
                            columns.len(),
                            out_array_ptrs.len()
                        ),
                    ));
                }
                for (i, column) in columns.iter().enumerate() {
                    arrow_bridge::export_column(&column.to_data(), out_array_ptrs[i])?;
                }
                Ok(Some(batch.num_rows()))
            }
            None => Ok(None),
        }
    }

    pub fn result_cancel(&self, qid: u64) -> Result<()> {
        let handle = self.get_query(qid)?;
        handle.cancel();
        Ok(())
    }

    pub fn result_stats(&self, qid: u64) -> Result<String> {
        let handle = self.get_query(qid)?;
        Ok(format!(
            r#"{{"produceNanos":{},"rows":{},"finished":{},"cancelled":{}}}"#,
            handle.produce_nanos.load(Ordering::Relaxed),
            handle.produced_rows.load(Ordering::Relaxed),
            handle.is_finished(),
            handle.is_cancelled()
        ))
    }

    /// Drops the query handle: cancels the producer task and releases everything.
    ///
    /// Idempotent: an already-drained query (e.g. `engineClose` cancelled it) is a
    /// success, so a Java `QueryResult.close()` racing an engine close does not throw.
    pub fn close_result(&self, qid: u64) -> Result<()> {
        self.queries.lock().unwrap().remove(&qid);
        Ok(())
    }

    fn get_query(&self, qid: u64) -> Result<Arc<QueryHandle>> {
        self.queries.lock().unwrap().get(&qid).cloned().ok_or_else(|| self.unknown_query_error(qid))
    }

    fn unknown_query_error(&self, qid: u64) -> EngineError {
        EngineError::new(
            ErrorKind::EngineClosed,
            format!("query {qid} is not registered (already closed or unknown)"),
        )
    }

    /// Executes SQL and writes the result to Parquet at `destination`, entirely in Rust.
    /// Returns (encode_nanos, publish_nanos) for benchmarking.
    pub fn query_to_parquet(&self, sql: &str, destination: &str) -> Result<(u64, u64)> {
        sql_guard::validate_select_only(sql)?;
        let (schema, plan) = self.plan(sql)?;
        // execute_stream spawns DataFusion runtime helpers internally, which requires a
        // tokio runtime context: enter the runtime for the call.
        let stream = self.rt.block_on(async {
            datafusion::physical_plan::execute_stream(plan, self.ctx.state().task_ctx())
        })?;
        let compression = parquet::compression_from_name(
            self.config.parquet_compression.as_deref().unwrap_or("snappy"),
        )?;
        let scratch_dir = self.config.scratch_dir.clone().unwrap_or_else(|| {
            std::env::temp_dir().to_string_lossy().to_string()
        });
        let (scratch, encode_ns) = parquet::encode_to_scratch(
            self.rt.handle(),
            schema,
            stream,
            std::path::Path::new(&scratch_dir),
            compression,
        )?;
        let store = self.resolve_store(destination)?;
        let publish_ns = parquet::publish(self.rt.handle(), &scratch, destination, store)?;
        Ok((encode_ns, publish_ns))
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Cancel all live queries first so producers stop touching shared state.
        for (_, handle) in self.queries.lock().unwrap().drain() {
            handle.cancel();
        }
        // Drop SessionContext -> MemTables (all registered batches) are released.
    }
}