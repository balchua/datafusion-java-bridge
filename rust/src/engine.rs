//! The engine: owns the tokio runtime, the DataFusion SessionContext, staged Arrow
//! tables, live queries and the object-store registry. One engine == one payment job.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SQLOptions, SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::ParquetReadOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::arrow_bridge;
use crate::error::{EngineError, ErrorKind, Result};
use crate::object_store::{ObjectStoreRegistry, S3Config};
use crate::parquet;
use crate::query::QueryHandle;
use crate::sql_guard;
use crate::sync::MutexExt;
use std::io::Write;

/// Captured at compile time by build.rs.
pub const RUSTC_VERSION: &str = match option_env!("DFE_RUSTC_VERSION") {
    Some(v) => v,
    None => "unknown",
};

fn default_true() -> bool {
    true
}

/// Upper bound on worker threads / target partitions, guarding against a
/// misconfigured `parallelism` spawning an unreasonable number of OS threads.
const MAX_PARALLELISM: usize = 256;

/// Stack size for the dedicated query-planning thread. Planning recurses over the
/// expression tree; a deep chain must not overflow the (smaller) JNI caller stack.
const PLAN_STACK_BYTES: usize = 64 * 1024 * 1024;

/// Configuration passed as JSON from Java. Defaults are applied in `EngineConfig::load`.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
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
    /// Validate imported Arrow arrays (offsets, UTF-8, dictionary keys) with
    /// `ArrayData::validate_full()` before DataFusion reads them. **Default `false`**: the
    /// O(rows) scan is off so registration stays flat; enable it when the producer may
    /// supply malformed data (the C Data Interface import is otherwise unchecked). Deep-copy
    /// ingest always validates, because it reads the declared buffers/offsets.
    #[serde(default)]
    pub validate_ingest: bool,
    /// Expose DataFusion's `information_schema` (table/settings metadata) to SQL.
    /// Default `false` (opt-in): it reveals session/engine settings.
    #[serde(default)]
    pub information_schema: bool,
    /// Local filesystem roots a Parquet destination/registration must resolve under.
    /// Empty (default) = no restriction. When set, `..`/symlink escapes are rejected
    /// (the path is canonicalized and must lie under one of the roots).
    #[serde(default)]
    pub allowed_roots: Vec<String>,
    /// Overwrite an existing Parquet destination. Default `false`: refusing to clobber
    /// avoids a destructive write to a wrong path. <b>Enforced for local destinations
    /// only</b>; object stores (s3://) are always overwritten.
    #[serde(default)]
    pub overwrite_parquet: bool,
    /// Max bytes DataFusion may spill to the scratch directory. Absent = DataFusion default.
    pub spill_dir_max_bytes: Option<u64>,
    /// Planning timeout in milliseconds. Absent = no timeout.
    pub plan_timeout_millis: Option<u64>,
    /// How long `engineClose` waits for in-flight native calls to return before giving up.
    /// Milliseconds. Absent = 60_000. `0` = wait forever (never give up, but a hung call can
    /// block close indefinitely). If the deadline expires the engine is **not** freed now —
    /// an in-flight call's `Arc` keeps it alive and it is released when that call finishes.
    pub close_timeout_millis: Option<u64>,
}

pub struct Engine {
    config: EngineConfig,
    rt: tokio::runtime::Runtime,
    ctx: SessionContext,
    staged: Mutex<HashMap<u64, StagedTable>>,
    queries: Mutex<HashMap<u64, Arc<QueryHandle>>>,
    object_stores: Mutex<ObjectStoreRegistry>,
    next_id: AtomicU64,
    /// `None` = wait forever for in-flight calls; `Some` = give up and leak after this long.
    close_timeout: Option<std::time::Duration>,
    /// Planning timeout; `None` = no timeout.
    plan_timeout: Option<std::time::Duration>,
    /// Cancels non-query operations (`queryToParquet`, `registerParquet`) that run
    /// synchronously on a JNI thread. Tripped by `engineClose` before it waits idle.
    shutdown: CancellationToken,
    /// Number of native calls currently inside this engine. `engineClose` waits for
    /// this to reach 0 after cancelling queries, so an in-flight `result_next` on a
    /// foreign thread can never touch a freed engine.
    ///
    /// Private: only [`ActiveCall`] may mutate it, so the guard can never be unbalanced.
    active_calls: AtomicU64,
}

/// RAII guard for `Engine::active_calls`. Holds a shared reference to the engine, so an
/// in-flight call keeps it alive even after `engineClose` removes it from the registry.
pub struct ActiveCall {
    _engine: Arc<Engine>,
}

impl ActiveCall {
    /// Increments `active_calls` and returns a guard that decrements it on drop.
    ///
    /// Constructing a guard without matching it with a drop leaks the count and would
    /// wedge `engineClose`'s `wait_idle` forever, so this is the only place the counter
    /// is incremented. Callers obtain guards through `ffi::engine_from`, which does the
    /// registry validation under the same lock.
    pub(crate) fn new(engine: Arc<Engine>) -> Self {
        engine.active_calls.fetch_add(1, Ordering::SeqCst);
        Self { _engine: engine }
    }
}

impl Drop for ActiveCall {
    fn drop(&mut self) {
        self._engine.active_calls.fetch_sub(1, Ordering::SeqCst);
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
        for (_, handle) in self.queries.lock_or_recover().drain() {
            handle.cancel();
        }
    }

    /// Cancels non-query operations (`queryToParquet`, `registerParquet`) currently running
    /// on a JNI thread. Called by `engineClose` before it waits for `active_calls` to drain.
    /// Each operation races the token via `tokio::select!`, so cancellation drops the
    /// in-flight future (closing any connection) instead of waiting on the network.
    pub fn cancel_all_operations(&self) {
        self.shutdown.cancel();
    }

    /// Number of native calls currently executing against this engine.
    pub fn active_calls(&self) -> u64 {
        self.active_calls.load(Ordering::SeqCst)
    }

    /// Blocks until all in-flight native calls returned, or the configured deadline
    /// expires. Returns `true` if the engine is idle, `false` if a call is still running
    /// (the engine is kept alive by that call's `Arc` and freed when it finishes).
    ///
    /// Called with the handle already removed from the registry, so no new calls can
    /// enter and `active_calls` cannot increase. The deadline comes from
    /// `EngineConfig::close_timeout_millis` (default 60 s; `None` waits forever).
    pub fn wait_idle(&self) -> bool {
        let deadline = self.close_timeout.map(|d| std::time::Instant::now() + d);
        while self.active_calls() > 0 {
            if let Some(dl) = deadline {
                if std::time::Instant::now() > dl {
                    // Do not block forever on a stuck call; the engine stays alive via its
                    // Arc and is freed when that call finishes.
                    return false;
                }
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
        } else if config.parallelism > MAX_PARALLELISM {
            // Sanity clamp: an unbounded user value would spawn that many OS threads.
            let _ = writeln!(
                std::io::stderr(),
                "datafusion-java-bridge: parallelism {} exceeds the maximum ({}); clamping",
                config.parallelism,
                MAX_PARALLELISM
            );
            MAX_PARALLELISM
        } else {
            config.parallelism
        };

        let scratch_dir = config
            .scratch_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().to_string_lossy().to_string());

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(parallelism)
            // Larger worker stacks: DataFusion evaluates/drops the (possibly deep) physical
            // expression tree on worker threads, which would otherwise overflow 2 MB.
            .thread_stack_size(8 * 1024 * 1024)
            .enable_all()
            .thread_name("df-engine")
            .build()?;

        let mut rt_builder = RuntimeEnvBuilder::new().with_temp_file_path(&scratch_dir);
        if config.memory_limit_bytes > 0 {
            rt_builder = rt_builder.with_memory_limit(config.memory_limit_bytes as usize, 1.0);
        }
        if let Some(bytes) = config.spill_dir_max_bytes {
            // Cap how much DataFusion may spill to the scratch directory.
            rt_builder = rt_builder.with_max_temp_directory_size(bytes);
        }
        let runtime_env = rt_builder
            .build_arc()
            .map_err(|e| EngineError::new(ErrorKind::Resource, format!("runtime env: {e}")))?;

        let session_config = SessionConfig::new()
            .with_information_schema(config.information_schema)
            .with_target_partitions(parallelism);

        let ctx = SessionContext::new_with_config_rt(session_config, runtime_env);
        let s3 = config.s3.clone();
        let close_timeout = match config.close_timeout_millis {
            Some(0) => None, // 0 = wait forever for in-flight calls
            Some(ms) => Some(std::time::Duration::from_millis(ms)),
            None => Some(std::time::Duration::from_secs(60)),
        };
        // 0 means "no timeout" (mirrors close_timeout_millis).
        let plan_timeout = config
            .plan_timeout_millis
            .filter(|ms| *ms > 0)
            .map(std::time::Duration::from_millis);

        Ok(Engine {
            config,
            rt,
            ctx,
            staged: Mutex::new(HashMap::new()),
            queries: Mutex::new(HashMap::new()),
            object_stores: Mutex::new(ObjectStoreRegistry::new(s3)),
            next_id: AtomicU64::new(1),
            close_timeout,
            plan_timeout,
            shutdown: CancellationToken::new(),
            active_calls: AtomicU64::new(0),
        })
    }

    /// Imports a record batch exported by Java and stages it under an internal id.
    /// Java may free its vectors as soon as this call returns.
    pub fn stage_arrow(&self, table: &str, schema_ptr: usize, array_ptrs: &[usize]) -> Result<u64> {
        if table.is_empty() {
            return Err(EngineError::new(
                ErrorKind::SchemaMismatch,
                "empty table name",
            ));
        }
        let (schema, batch) = arrow_bridge::import_batch(
            schema_ptr,
            array_ptrs,
            !self.config.zero_copy,
            self.config.validate_ingest,
        )?;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.staged.lock_or_recover().insert(
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
            .lock_or_recover()
            .remove(&staged_id)
            .ok_or_else(|| {
                EngineError::new(
                    ErrorKind::Internal,
                    format!("unknown staged table id {staged_id}"),
                )
            })?;
        let mem = MemTable::try_new(staged.schema, vec![staged.batches])
            .map_err(|e| EngineError::new(ErrorKind::SchemaMismatch, format!("mem table: {e}")))?;
        self.ctx.register_table(table, Arc::new(mem)).map_err(|e| {
            EngineError::new(
                ErrorKind::SchemaMismatch,
                format!("registration failed: {e}"),
            )
        })?;
        Ok(())
    }

    /// Registers a Parquet-backed table. Parquet decoding stays entirely in DataFusion.
    pub fn register_parquet(&self, table: &str, location: &str) -> Result<()> {
        reject_url_query_fragment(location)?;
        self.enforce_allowed_root(location)?;
        self.ensure_object_store(location)?;
        let cancel = self.shutdown.clone();
        self.rt.block_on(async {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(EngineError::closing()),
                r = self.ctx.register_parquet(table, location, ParquetReadOptions::default()) => {
                    r.map_err(Into::into)
                }
            }
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
        let resolved = self.object_stores.lock_or_recover().store_for(&url)?;
        if let Some((store_url, store)) = resolved {
            self.ctx.register_object_store(&store_url, store);
        }
        Ok(())
    }

    fn resolve_store(
        &self,
        destination: &str,
    ) -> Result<Option<Arc<dyn object_store::ObjectStore>>> {
        match crate::dest::classify(destination)? {
            crate::dest::Destination::Local(_) => Ok(None),
            crate::dest::Destination::Remote(url) => Ok(self
                .object_stores
                .lock_or_recover()
                .store_for(&url)?
                .map(|(_, store)| store)),
        }
    }

    /// Rejects a local destination that escapes the configured `allowed_roots`.
    ///
    /// No-op when `allowed_roots` is empty (the default). When set, the destination is
    /// canonicalized (as far as it exists) and must lie under one of the roots, so `..`
    /// components and symlink escapes are rejected.
    fn enforce_allowed_root(&self, destination: &str) -> Result<()> {
        if self.config.allowed_roots.is_empty() {
            return Ok(());
        }
        let path = match crate::dest::classify(destination)? {
            crate::dest::Destination::Local(p) => p,
            // A remote destination is not confined by local roots.
            crate::dest::Destination::Remote(_) => return Ok(()),
        };
        let canon = canonicalize_lenient(&path)?;
        for root in &self.config.allowed_roots {
            if let Ok(root_canon) = std::fs::canonicalize(root) {
                if canon.starts_with(&root_canon) {
                    return Ok(());
                }
            }
        }
        Err(EngineError::new(
            ErrorKind::Parquet,
            format!("destination is outside the configured allowed roots: {destination}"),
        ))
    }

    /// Validates SQL, plans the query (parse -> logical -> optimize -> physical) and
    /// starts execution on the runtime. Returns the query id.
    pub fn query(&self, sql: &str) -> Result<u64> {
        let (schema, plan) = self.plan_with_large_stack(sql)?;
        let qid = self.next_id.fetch_add(1, Ordering::SeqCst);
        let handle =
            QueryHandle::start(self.rt.handle(), schema, plan, self.ctx.state().task_ctx());
        self.queries.lock_or_recover().insert(qid, handle);
        Ok(qid)
    }

    /// Plans the query on a dedicated thread with a large stack.
    ///
    /// `sqlparser`/DataFusion recurse over expression trees; a deeply chained SQL
    /// expression (thousands of `OR` terms) overflows a normal stack, and a Rust stack
    /// overflow **aborts the whole process** (it cannot be caught by `catch_unwind`).
    /// Planning therefore must not run on the JVM-owned JNI caller thread, whose stack is
    /// smaller than this.
    fn plan_with_large_stack(
        &self,
        sql: &str,
    ) -> Result<(SchemaRef, Arc<dyn datafusion::physical_plan::ExecutionPlan>)> {
        std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("df-plan".to_string())
                .stack_size(PLAN_STACK_BYTES)
                .spawn_scoped(scope, || {
                    // Parse/validate on this large-stack thread too: sqlparser builds and
                    // then drops a recursive AST, which overflows the JNI caller's smaller
                    // stack on a pathologically deep expression (and a stack overflow aborts
                    // the process).
                    sql_guard::validate_select_only(sql)?;
                    self.rt.block_on(async {
                        match self.plan_timeout {
                            Some(t) => tokio::time::timeout(t, self.plan_async(sql))
                                .await
                                .unwrap_or_else(|_| {
                                    Err(EngineError::new(
                                        ErrorKind::Resource,
                                        "query planning timed out",
                                    ))
                                }),
                            None => self.plan_async(sql).await,
                        }
                    })
                })
                .map_err(|e| {
                    EngineError::new(ErrorKind::Internal, format!("spawn planning thread: {e}"))
                })?
                .join()
                .unwrap_or_else(|_| {
                    Err(EngineError::new(
                        ErrorKind::Internal,
                        "query planning panicked",
                    ))
                })
        })
    }

    /// Planning phase. Returns (result schema, physical plan).
    async fn plan_async(
        &self,
        sql: &str,
    ) -> Result<(SchemaRef, Arc<dyn datafusion::physical_plan::ExecutionPlan>)> {
        let state = self.ctx.state();
        let logical = state.create_logical_plan(sql).await?;
        select_only_options().verify_plan(&logical)?;
        let physical = state.create_physical_plan(&logical).await?;
        Ok((physical.schema(), physical))
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
                // All-or-nothing: build every column's FFI struct first, then write them.
                // A failure part-way can therefore never leave live FFI structs in Java
                // memory that the caller has no way to release.
                let owned: Vec<datafusion::arrow::array::ArrayData> =
                    columns.iter().map(|c| c.to_data()).collect();
                let data: Vec<&datafusion::arrow::array::ArrayData> = owned.iter().collect();
                arrow_bridge::export_columns_atomic(&data, out_array_ptrs)?;
                Ok(Some(batch.num_rows()))
            }
            None => Ok(None),
        }
    }

    /// Cancels a running query.
    ///
    /// Idempotent: cancelling an already-finished/closed or unknown query id is a
    /// success, not an error. This lets a Java watchdog thread call `cancel()` while the
    /// consuming thread calls `close()` without a race ever surfacing as an exception.
    pub fn result_cancel(&self, qid: u64) -> Result<()> {
        if let Some(handle) = self.queries.lock_or_recover().get(&qid).cloned() {
            handle.cancel();
        }
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
        self.queries.lock_or_recover().remove(&qid);
        Ok(())
    }

    fn get_query(&self, qid: u64) -> Result<Arc<QueryHandle>> {
        self.queries
            .lock_or_recover()
            .get(&qid)
            .cloned()
            .ok_or_else(|| self.unknown_query_error(qid))
    }

    fn unknown_query_error(&self, qid: u64) -> EngineError {
        EngineError::new(
            ErrorKind::QueryNotFound,
            format!("query {qid} is not registered (already closed or unknown)"),
        )
    }

    /// Executes SQL and writes the result to Parquet at `destination`, entirely in Rust.
    /// Returns (encode_nanos, publish_nanos) for benchmarking.
    pub fn query_to_parquet(&self, sql: &str, destination: &str) -> Result<(u64, u64)> {
        // Validate the destination up front, so a bad path/scheme/bucket fails before the
        // query runs (validation + planning parse on a large-stack thread).
        reject_url_query_fragment(destination)?;
        self.enforce_allowed_root(destination)?;
        let store = self.resolve_store(destination)?;
        let (schema, plan) = self.plan_with_large_stack(sql)?;
        self.rt.block_on(async {
            // Build the stream inside the runtime: DataFusion spawns tasks for
            // multi-partition plans, which requires a Tokio context.
            let stream =
                datafusion::physical_plan::execute_stream(plan, self.ctx.state().task_ctx())?;
            self.encode_and_publish(schema, stream, destination, store)
                .await
        })
    }

    async fn encode_and_publish(
        &self,
        schema: SchemaRef,
        stream: datafusion::physical_plan::SendableRecordBatchStream,
        destination: &str,
        store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Result<(u64, u64)> {
        let compression = parquet::compression_from_name(
            self.config
                .parquet_compression
                .as_deref()
                .unwrap_or("snappy"),
        )?;
        let scratch_dir = self
            .config
            .scratch_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().to_string_lossy().to_string());
        let (scratch, encode_ns) = parquet::encode_to_scratch(
            schema,
            stream,
            std::path::Path::new(&scratch_dir),
            compression,
            &self.shutdown,
        )
        .await?;
        // Re-check confinement immediately before writing: the check at call start is a
        // TOCTOU window across the potentially slow plan/encode steps.
        self.enforce_allowed_root(destination)?;
        let publish_ns = parquet::publish(
            scratch.path(),
            destination,
            store,
            &self.shutdown,
            self.config.overwrite_parquet,
        )
        .await?;
        Ok((encode_ns, publish_ns))
    }
}

/// Rejects `?`/`#` in URL destinations: `Url::parse` silently drops them, so the write would
/// land on a different key than the caller wrote.
fn reject_url_query_fragment(destination: &str) -> Result<()> {
    if destination.contains("://") && (destination.contains('?') || destination.contains('#')) {
        return Err(EngineError::new(
            ErrorKind::ObjectStore,
            "destination URL must not contain '?' or '#'",
        ));
    }
    Ok(())
}

/// Canonicalizes `path` as far as it exists (so a not-yet-created destination can still be
/// checked), rejecting any `..` component.
fn canonicalize_lenient(path: &std::path::Path) -> Result<std::path::PathBuf> {
    use std::path::Component;
    if path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(EngineError::new(
            ErrorKind::Parquet,
            format!("path must not contain '..': {}", path.display()),
        ));
    }
    // Make relative paths absolute against the current directory so they can be compared
    // to an absolute root.
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| EngineError::new(ErrorKind::Parquet, format!("cannot resolve cwd: {e}")))?
            .join(path)
    };
    let mut existing = absolute;
    let mut suffix: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match std::fs::canonicalize(&existing) {
            Ok(c) => {
                let mut out = c;
                for part in suffix.iter().rev() {
                    out.push(part);
                }
                return Ok(out);
            }
            // Only "not found" means "does not exist yet": keep walking up.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => match existing.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => {
                    if let Some(name) = existing.file_name() {
                        suffix.push(name.to_os_string());
                    }
                    existing = parent.to_path_buf();
                }
                _ => {
                    let mut out = existing;
                    for part in suffix.iter().rev() {
                        out.push(part);
                    }
                    return Ok(out);
                }
            },
            // Fail closed on any other error (EACCES, ELOOP, ...): a symlink loop or an
            // unreadable directory must not be treated as "does not exist".
            Err(e) => return Err(e.into()),
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        // Cancel all live queries first so producers stop touching shared state.
        for (_, handle) in self.queries.lock_or_recover().drain() {
            handle.cancel();
        }
        self.shutdown.cancel();
        // Drop SessionContext -> MemTables (all registered batches) are released.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryHandle;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
    use datafusion::physical_plan::SendableRecordBatchStream;
    use std::time::{Duration, Instant};

    fn engine_with(config: &str) -> Engine {
        Engine::create(config).unwrap()
    }

    /// A partition whose stream never yields a batch: the producer parks on `next().await`
    /// forever, so `JoinHandle::abort()` interrupts it immediately and deterministically.
    #[derive(Debug)]
    struct PendingPartition {
        schema: SchemaRef,
    }

    impl PartitionStream for PendingPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::pending::<
                    std::result::Result<RecordBatch, datafusion::error::DataFusionError>,
                >(),
            ))
        }
    }

    #[test]
    fn active_call_guard_increments_and_balances() {
        let engine = Arc::new(engine_with("{}"));
        assert_eq!(engine.active_calls(), 0);
        {
            let _g = ActiveCall::new(Arc::clone(&engine));
            assert_eq!(engine.active_calls(), 1);
            let _g2 = ActiveCall::new(Arc::clone(&engine));
            assert_eq!(engine.active_calls(), 2);
        }
        assert_eq!(engine.active_calls(), 0);
    }

    #[test]
    fn wait_idle_returns_immediately_when_idle() {
        let engine = engine_with("{}");
        assert!(engine.wait_idle());
    }

    #[test]
    fn wait_idle_times_out_on_stuck_call_then_succeeds() {
        // A short deadline keeps the test fast; the default is 60 s.
        let engine = engine_with(r#"{"close_timeout_millis":30}"#);
        engine.active_calls.fetch_add(1, Ordering::SeqCst);
        let start = Instant::now();
        assert!(!engine.wait_idle(), "should give up and report not-idle");
        assert!(start.elapsed() >= Duration::from_millis(30));
        // Drain the simulated in-flight call; the engine is now safe to free.
        engine.active_calls.fetch_sub(1, Ordering::SeqCst);
        assert!(engine.wait_idle());
    }

    #[test]
    fn cancel_all_operations_trips_the_shutdown_token() {
        let engine = engine_with("{}");
        assert!(!engine.shutdown.is_cancelled());
        engine.cancel_all_operations();
        assert!(engine.shutdown.is_cancelled());
    }

    #[test]
    fn cancelling_a_running_query_yields_cancelled() {
        let engine = engine_with("{}");
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        // A plan whose only partition never produces a batch, so the producer parks on
        // `stream.next().await` and is aborted immediately when cancelled.
        let exec = StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(PendingPartition {
                schema: schema.clone(),
            })],
            None,
            std::iter::empty(),
            true,
            None,
        )
        .unwrap();
        let handle = QueryHandle::start(
            engine.rt.handle(),
            schema.clone(),
            Arc::new(exec),
            engine.ctx.state().task_ctx(),
        );

        let canceller = {
            let h = handle.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                h.cancel();
            })
        };

        // Abort drops the producer's sender, closing the channel. Because the query was
        // cancelled, the closed stream must surface a Cancelled error, not end-of-stream.
        let err = handle.next().unwrap_err();
        canceller.join().unwrap();
        assert_eq!(err.kind, ErrorKind::Cancelled);
    }

    #[test]
    fn result_cancel_unknown_id_is_ok() {
        let engine = engine_with("{}");
        assert!(engine.result_cancel(999_999).is_ok());
    }

    #[test]
    fn result_cancel_is_idempotent() {
        let engine = engine_with("{}");
        let qid = engine.query("SELECT 1 AS n").unwrap();
        assert!(engine.result_cancel(qid).is_ok());
        assert!(
            engine.result_cancel(qid).is_ok(),
            "second cancel must also be ok"
        );
    }

    #[test]
    fn query_to_parquet_handles_multi_partition_plan_and_no_clobber() {
        // H1 regression: a UNION ALL is multi-partition; building the stream must happen
        // inside the runtime. Also exercises overwrite=false (default).
        let dir = std::env::temp_dir().join(format!("dfe-qtp-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let engine = engine_with("{}");
        let dest = dir.join("out.parquet");

        let first = engine.query_to_parquet(
            "SELECT 1 AS a UNION ALL SELECT 2 UNION ALL SELECT 3",
            dest.to_str().unwrap(),
        );
        assert!(first.is_ok(), "query_to_parquet failed: {first:?}");
        assert!(dest.exists());

        // Second write to the same path must be refused (overwrite disabled by default).
        let second = engine.query_to_parquet("SELECT 1 AS a", dest.to_str().unwrap());
        assert!(
            second.is_err(),
            "no-clobber must reject an existing destination"
        );

        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn allowed_roots_confines_local_destinations() {
        let root = std::env::temp_dir().join(format!("dfe-root-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let engine = engine_with(&format!(r#"{{"allowed_roots":["{}"]}}"#, root.display()));

        let inside = root.join("in.parquet");
        assert!(
            engine
                .query_to_parquet("SELECT 1 AS a", inside.to_str().unwrap())
                .is_ok(),
            "a destination under the allowed root must be accepted"
        );

        let outside =
            std::env::temp_dir().join(format!("dfe-outside-{}.parquet", std::process::id()));
        assert!(
            engine
                .query_to_parquet("SELECT 1 AS a", outside.to_str().unwrap())
                .is_err(),
            "a destination outside the allowed root must be rejected"
        );
        // A `file:` URL form must be confined too (not read as a relative path).
        let outside_url = format!("file://{}", outside.display());
        assert!(
            engine
                .query_to_parquet("SELECT 1 AS a", &outside_url)
                .is_err(),
            "a file: URL outside the allowed root must be rejected"
        );

        let _ = std::fs::remove_file(&inside);
        let _ = std::fs::remove_dir(&root);
    }
}
