//! JNI entry points. This is the only module that touches the JVM directly.
//!
//! Conventions:
//! * Every entry point body runs inside [`run`], which catches panics and throws any
//!   `EngineError` as the Java exception returned by `EngineError::java_class()`, then
//!   returns the function's sentinel value (`0` for handles, `-2` for row counts). Without
//!   this, a Rust panic would unwind across the `extern "system"` boundary and abort the JVM.
//! * Engine handles are opaque, never-reused `u64` ids validated through a global registry,
//!   so a stale or garbage pointer throws instead of crashing.
//! * Query ids are engine-local `u64` counters validated through the engine's registry, so
//!   the result methods take `(engineHandle, queryId)`.

use jni::objects::{JClass, JLongArray, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use crate::engine::{ActiveCall, Engine};
use crate::error::{EngineError, ErrorKind, Result};

/// Registry of live engines, keyed by an opaque, never-reused id. Ids are handed to Java as
/// `jlong` handles; the raw pointer is never exposed, so a stale handle cannot be confused
/// with a later engine that happens to reuse the same address (ABA).
static ENGINES: LazyLock<Mutex<HashMap<u64, Arc<Engine>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Source of engine ids. Starts at 1 so 0 can never be a valid handle.
static NEXT_ENGINE_ID: AtomicU64 = AtomicU64::new(1);

/// Upper bound on live engines, so a caller cannot spawn unbounded runtimes/threads.
const MAX_ENGINES: usize = 1024;

const ENGINE_CLOSED: &str = "engine is closed or the handle is invalid";
const SENTINEL_HANDLE: jlong = 0; // handle-returning methods: error thrown
const SENTINEL_ERROR: jlong = -2; // row-count methods: error thrown
const END_OF_STREAM: jlong = -1; // row-count methods: clean end of stream

fn next_engine_id() -> u64 {
    NEXT_ENGINE_ID.fetch_add(1, Ordering::Relaxed)
}

/// Locks the engine registry, recovering from a poisoned mutex.
///
/// The guarded value is a `HashMap` with no cross-entry invariant, so a panic mid-section
/// cannot leave it dangerously inconsistent. Recovering means a single bad call fails
/// instead of bricking every engine in the process (the registry lock is shared by all
/// engines).
fn engines() -> std::sync::MutexGuard<'static, HashMap<u64, Arc<Engine>>> {
    ENGINES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn closed_error() -> EngineError {
    EngineError::new(ErrorKind::EngineClosed, ENGINE_CLOSED)
}

/// Converts a caught panic payload into a Java-visible internal error.
fn panic_error(payload: &(dyn std::any::Any + Send)) -> EngineError {
    let message = if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    };
    EngineError::new(ErrorKind::Internal, format!("native panic: {message}"))
}

/// Runs a JNI entry-point body, turning a panic or an `Err` into a thrown Java exception.
///
/// On success the body's value is returned; on failure `default` is returned with the Java
/// exception pending. This is the single place that catches panics and throws, so a panic
/// can never unwind across the `extern "system"` boundary (which would abort the JVM).
fn run<'local, T>(
    env: &mut JNIEnv<'local>,
    default: T,
    f: impl FnOnce(&mut JNIEnv<'local>) -> Result<T>,
) -> T {
    // SAFETY: the body runs to completion before `env` is reborrowed for throwing, and the
    // JNI env is only used on this thread, so there is no aliasing or cross-thread use.
    let env_ptr: *mut JNIEnv<'local> = env;
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe { f(&mut *env_ptr) }));
    match outcome {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => {
            throw_unless_pending(unsafe { &mut *env_ptr }, &err);
            default
        }
        Err(payload) => {
            let err = panic_error(&*payload);
            throw_unless_pending(unsafe { &mut *env_ptr }, &err);
            default
        }
    }
}

/// Throws `err` unless a Java exception is already pending (throwing while one is pending
/// is invalid JNI; the pending exception is the more precise one).
fn throw_unless_pending(env: &mut JNIEnv, err: &EngineError) {
    if env.exception_check().unwrap_or(false) {
        return;
    }
    throw(env, err);
}

/// # Safety invariant
///
/// `handle` must be a value previously returned by `engineCreate` that has not been passed
/// to `engineClose`. The registry guarantees the id maps to a live engine; the returned
/// `Arc` keeps it alive for the duration of the call, and `active_calls` is incremented
/// under the registry lock so `engineClose`'s `wait_idle` sees in-flight calls.
fn engine_from(handle: jlong) -> Result<(ActiveCall, Arc<Engine>)> {
    let registry = engines();
    let engine = registry
        .get(&(handle as u64))
        .cloned()
        .ok_or_else(closed_error)?;
    Ok((ActiveCall::new(Arc::clone(&engine)), engine))
}

fn get_string(env: &mut JNIEnv, s: &JString) -> Result<String> {
    env.get_string(s)
        .map(|j| j.to_string_lossy().into_owned())
        .map_err(|e| EngineError::new(ErrorKind::Internal, format!("jni string access: {e}")))
}

fn get_addresses(env: &mut JNIEnv, arr: &JLongArray) -> Result<Vec<usize>> {
    let len = env
        .get_array_length(arr)
        .map_err(|e| EngineError::new(ErrorKind::Internal, format!("jni array length: {e}")))?;
    if len < 0 {
        // A negative length would overflow the `Vec` capacity below and panic.
        return Err(EngineError::new(
            ErrorKind::Internal,
            format!("negative JNI array length: {len}"),
        ));
    }
    // Defense in depth: these arrays hold one entry per result column; a huge length would
    // attempt a correspondingly large allocation.
    const MAX_LEN: i32 = 65_536;
    if len > MAX_LEN {
        return Err(EngineError::new(
            ErrorKind::Internal,
            format!("implausible JNI array length: {len}"),
        ));
    }
    let mut longs = vec![0i64; len as usize];
    env.get_long_array_region(arr, 0, &mut longs)
        .map_err(|e| EngineError::new(ErrorKind::Internal, format!("jni array access: {e}")))?;
    Ok(longs.into_iter().map(|v| v as usize).collect())
}

fn throw(env: &mut JNIEnv, err: &EngineError) {
    // The Java exception class already encodes the kind, so throw the bare message.
    let _ = env.throw_new(err.java_class(), &err.message);
}

fn new_string<'local>(env: &mut JNIEnv<'local>, value: &str) -> Result<JString<'local>> {
    env.new_string(value)
        .map_err(|e| EngineError::new(ErrorKind::Internal, format!("jni new string: {e}")))
}

// ---------------------------------------------------------------------------
// engine lifecycle
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_engineCreate(
    mut env: JNIEnv,
    _class: JClass,
    config_json: JString,
) -> jlong {
    run(&mut env, SENTINEL_HANDLE, |env| {
        let config = get_string(env, &config_json)?;
        // Cheap pre-check so a rejected create does not build and tear down a runtime.
        if engines().len() >= MAX_ENGINES {
            return Err(EngineError::new(
                ErrorKind::Resource,
                format!("too many live engines (max {MAX_ENGINES})"),
            ));
        }
        let engine = Engine::create(&config)?;
        let mut registry = engines();
        if registry.len() >= MAX_ENGINES {
            return Err(EngineError::new(
                ErrorKind::Resource,
                format!("too many live engines (max {MAX_ENGINES})"),
            ));
        }
        let id = next_engine_id();
        registry.insert(id, Arc::new(engine));
        Ok(id as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_engineClose(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    run(&mut env, (), |_env| {
        // Remove from the registry first: after this no new call can enter, and the engine
        // is freed once the last in-flight call drops its `Arc` reference.
        let engine = match engines().remove(&(handle as u64)) {
            Some(e) => e,
            None => return Err(closed_error()),
        };
        engine.cancel_all_queries();
        engine.cancel_all_operations();
        if !engine.wait_idle() {
            // An in-flight call outlived the deadline; it holds its own `Arc`, so the engine
            // is freed when that call finishes — no use-after-free, no leak.
            let _ = writeln!(
                std::io::stderr(),
                "datafusion-java-bridge: engineClose timed out waiting for in-flight native \
                 calls; the engine will be released when they finish"
            );
        }
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_engineVersion<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass,
) -> JString<'local> {
    run(&mut env, JString::default(), |env| {
        new_string(env, &Engine::version())
    })
}

// ---------------------------------------------------------------------------
// registration
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_stageArrow(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    table_name: JString,
    schema_addr: jlong,
    array_addrs: JLongArray,
) -> jlong {
    run(&mut env, SENTINEL_HANDLE, |env| {
        let (_guard, engine) = engine_from(handle)?;
        let table = get_string(env, &table_name)?;
        let addrs = get_addresses(env, &array_addrs)?;
        Ok(engine.stage_arrow(&table, schema_addr as usize, &addrs)? as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_commitTable(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    table_name: JString,
    staged_id: jlong,
) {
    run(&mut env, (), |env| {
        let (_guard, engine) = engine_from(handle)?;
        let table = get_string(env, &table_name)?;
        engine.commit_table(&table, staged_id as u64)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_registerParquet(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    table_name: JString,
    location: JString,
) {
    run(&mut env, (), |env| {
        let (_guard, engine) = engine_from(handle)?;
        let table = get_string(env, &table_name)?;
        let location = get_string(env, &location)?;
        engine.register_parquet(&table, &location)
    })
}

// ---------------------------------------------------------------------------
// queries
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_query(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    sql: JString,
) -> jlong {
    run(&mut env, SENTINEL_HANDLE, |env| {
        let (_guard, engine) = engine_from(handle)?;
        let sql = get_string(env, &sql)?;
        Ok(engine.query(&sql)? as jlong)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultSchema(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
    schema_addr: jlong,
) {
    run(&mut env, (), |_env| {
        let (_guard, engine) = engine_from(handle)?;
        engine.result_schema(query_id as u64, schema_addr as usize)
    })
}

/// Returns the number of rows in the next batch (>= 0), `-1` at end of stream, or `-2`
/// when an exception was thrown.
#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultNextBatch(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
    array_addrs: JLongArray,
) -> jlong {
    run(&mut env, SENTINEL_ERROR, |env| {
        let (_guard, engine) = engine_from(handle)?;
        let addrs = get_addresses(env, &array_addrs)?;
        match engine.result_next(query_id as u64, &addrs)? {
            Some(rows) => Ok(rows as jlong),
            None => Ok(END_OF_STREAM),
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultCancel(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
) {
    run(&mut env, (), |_env| {
        let (_guard, engine) = match engine_from(handle) {
            Ok(pair) => pair,
            // Cancelling after the engine closed is a valid teardown order; a watchdog
            // thread must not observe an exception for a query that is already gone.
            Err(e) if e.kind == ErrorKind::EngineClosed => return Ok(()),
            Err(e) => return Err(e),
        };
        engine.result_cancel(query_id as u64)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultStats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
) -> JString<'local> {
    run(&mut env, JString::default(), |env| {
        let (_guard, engine) = engine_from(handle)?;
        let stats = engine.result_stats(query_id as u64)?;
        new_string(env, &stats)
    })
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultClose(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
) {
    run(&mut env, (), |_env| {
        let (_guard, engine) = match engine_from(handle) {
            Ok(pair) => pair,
            // Closing a result after its engine was closed is a valid teardown order.
            Err(e) if e.kind == ErrorKind::EngineClosed => return Ok(()),
            Err(e) => return Err(e),
        };
        engine.close_result(query_id as u64)
    })
}

/// Returns a JSON stats string: {"encode_nanos":..,"publish_nanos":..}
#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_queryToParquet<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    handle: jlong,
    sql: JString,
    destination: JString,
) -> JString<'local> {
    run(&mut env, JString::default(), |env| {
        let (_guard, engine) = engine_from(handle)?;
        let sql = get_string(env, &sql)?;
        let destination = get_string(env, &destination)?;
        let (encode_ns, publish_ns) = engine.query_to_parquet(&sql, &destination)?;
        let json = format!(
            r#"{{"encode_nanos":{},"publish_nanos":{}}}"#,
            encode_ns, publish_ns
        );
        new_string(env, &json)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_handle_is_rejected() {
        match engine_from(0) {
            Err(e) => assert_eq!(e.kind, ErrorKind::EngineClosed),
            Ok(_) => panic!("a zero handle must be rejected"),
        }
    }

    #[test]
    fn panic_error_extracts_the_message() {
        let payload: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert!(panic_error(&*payload).message.contains("boom"));
    }
}
