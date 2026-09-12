//! JNI entry points. This is the only module that touches the JVM directly.
//!
//! Conventions:
//! * Errors: any `EngineError` is thrown as the Java exception returned by
//!   `EngineError::java_class()` and the native method returns a sentinel
//!   (`0` for handles, `-2` for row counts) with the exception pending.
//! * Engine handles are `i64` holding `Box<Engine>` raw pointers. A global registry
//!   validates every handle so a stale or garbage pointer throws instead of crashing.
//! * Query ids are engine-local `u64` counters validated through the engine's registry,
//!   so the result methods take `(engineHandle, queryId)`.

use jni::objects::{JClass, JLongArray, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use std::collections::HashSet;
use std::sync::{LazyLock, Mutex};

use crate::engine::{ActiveCall, Engine};
use crate::error::{EngineError, ErrorKind, Result};

/// Registry of live engine handles. An engine is inserted on `engineCreate` and removed
/// in `engineClose` before the `Box` is dropped, so a validated pointer is always alive.
static ENGINES: LazyLock<Mutex<HashSet<usize>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

const ENGINE_CLOSED: &str = "engine is closed or the handle is invalid";
const SENTINEL_ERROR: jlong = -2; // row-count methods: error thrown

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn closed_error() -> EngineError {
    EngineError::new(ErrorKind::EngineClosed, ENGINE_CLOSED)
}

/// # Safety
///
/// `handle` must be a value previously returned by `engineCreate` that has not been
/// passed to `engineClose` yet. The registry guarantees the pointer is a live
/// `Box<Engine>`; dereferencing is safe because close removes the pointer from the
/// registry (under the same mutex) before freeing it. If the invariant is violated
/// (e.g. a forged handle), the registry check fails and an error is returned instead
/// of a dangling dereference.
fn engine_from(handle: jlong) -> Result<(ActiveCall, &'static Engine)> {
    // The registry lock serializes entry against engineClose: either the handle is
    // still registered and we increment `active_calls` before releasing the lock, or
    // it was already removed and we return an error. There is no window in between.
    let engines = ENGINES.lock().unwrap();
    let ptr = handle as usize;
    if ptr == 0 || !engines.contains(&ptr) {
        return Err(closed_error());
    }
    // SAFETY: see function documentation.
    let engine = unsafe { &*(ptr as *const Engine) };
    engine.active_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Ok((ActiveCall::new(engine), engine))
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
    let mut longs = vec![0i64; len as usize];
    env.get_long_array_region(arr, 0, &mut longs)
        .map_err(|e| EngineError::new(ErrorKind::Internal, format!("jni array access: {e}")))?;
    Ok(longs.into_iter().map(|v| v as usize).collect())
}

fn throw(env: &mut JNIEnv, err: &EngineError) {
    let _ = env.throw_new(err.java_class(), err.to_string());
}

fn throw_msg(env: &mut JNIEnv, err: &EngineError) {
    let _ = env.throw_new(err.java_class(), &err.message);
}

fn new_string<'local>(
    env: &mut JNIEnv<'local>,
    value: &str,
) -> Result<JString<'local>> {
    env.new_string(value).map_err(|e| {
        EngineError::new(ErrorKind::Internal, format!("jni new string: {e}"))
    })
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
    let config = match get_string(&mut env, &config_json) {
        Ok(c) => c,
        Err(e) => {
            throw_msg(&mut env, &e);
            return 0;
        }
    };
    match Engine::create(&config) {
        Ok(engine) => {
            let ptr = Box::into_raw(Box::new(engine)) as usize;
            ENGINES.lock().unwrap().insert(ptr);
            ptr as jlong
        }
        Err(e) => {
            throw(&mut env, &e);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_engineClose(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let ptr = handle as usize;
    // Remove from the registry first: after this, no other thread can enter the engine.
    if !ENGINES.lock().unwrap().remove(&ptr) {
        throw(&mut env, &closed_error());
        return;
    }
    // SAFETY: the pointer was created by `Box::into_raw(Box<Engine>)` in `engineCreate`
    // and removed from the registry above, so exactly one close can reclaim it. In-flight
    // calls hold `active_calls > 0`; we cancel their queries and wait for them to return
    // before freeing the Box.
    let engine = unsafe { &*(ptr as *const Engine) };
    engine.cancel_all_queries();
    if engine.wait_idle() {
        unsafe { drop(Box::from_raw(ptr as *mut Engine)) };
    } else {
        // A native call is still running after the deadline (e.g. a hung object-store
        // operation in queryToParquet/registerParquet). Freeing the engine now would be
        // a use-after-free in that call, so leak it instead. This only happens if a call
        // genuinely never returns; leaking one engine is strictly safer than corrupting.
        eprintln!(
            "datafusion-java-bridge: engineClose timed out waiting for in-flight native \
             calls; leaking the engine to avoid a use-after-free"
        );
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_engineVersion<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass,
) -> JString<'local> {
    match new_string(&mut env, &Engine::version()) {
        Ok(s) => s,
        Err(e) => {
            throw_msg(&mut env, &e);
            JString::default()
        }
    }
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
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return 0;
        }
    };
    let table = match get_string(&mut env, &table_name) {
        Ok(t) => t,
        Err(e) => {
            throw_msg(&mut env, &e);
            return 0;
        }
    };
    let addrs = match get_addresses(&mut env, &array_addrs) {
        Ok(a) => a,
        Err(e) => {
            throw_msg(&mut env, &e);
            return 0;
        }
    };
    match engine.stage_arrow(&table, schema_addr as usize, &addrs) {
        Ok(id) => id as jlong,
        Err(e) => {
            throw(&mut env, &e);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_commitTable(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    table_name: JString,
    staged_id: jlong,
) {
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return;
        }
    };
    let table = match get_string(&mut env, &table_name) {
        Ok(t) => t,
        Err(e) => {
            throw_msg(&mut env, &e);
            return;
        }
    };
    if let Err(e) = engine.commit_table(&table, staged_id as u64) {
        throw(&mut env, &e);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_registerParquet(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    table_name: JString,
    location: JString,
) {
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return;
        }
    };
    let table = match get_string(&mut env, &table_name) {
        Ok(t) => t,
        Err(e) => {
            throw_msg(&mut env, &e);
            return;
        }
    };
    let location = match get_string(&mut env, &location) {
        Ok(l) => l,
        Err(e) => {
            throw_msg(&mut env, &e);
            return;
        }
    };
    if let Err(e) = engine.register_parquet(&table, &location) {
        throw(&mut env, &e);
    }
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
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return 0;
        }
    };
    let sql = match get_string(&mut env, &sql) {
        Ok(s) => s,
        Err(e) => {
            throw_msg(&mut env, &e);
            return 0;
        }
    };
    match engine.query(&sql) {
        Ok(id) => id as jlong,
        Err(e) => {
            throw(&mut env, &e);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultSchema(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
    schema_addr: jlong,
) {
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return;
        }
    };
    if let Err(e) = engine.result_schema(query_id as u64, schema_addr as usize) {
        throw(&mut env, &e);
    }
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
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return SENTINEL_ERROR;
        }
    };
    let addrs = match get_addresses(&mut env, &array_addrs) {
        Ok(a) => a,
        Err(e) => {
            throw_msg(&mut env, &e);
            return SENTINEL_ERROR;
        }
    };
    match engine.result_next(query_id as u64, &addrs) {
        Ok(Some(rows)) => rows as jlong,
        Ok(None) => -1,
        Err(e) => {
            throw(&mut env, &e);
            SENTINEL_ERROR
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultCancel(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
) {
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return;
        }
    };
    if let Err(e) = engine.result_cancel(query_id as u64) {
        throw(&mut env, &e);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultStats<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
) -> JString<'local> {
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return JString::default();
        }
    };
    match engine.result_stats(query_id as u64).and_then(|s| new_string(&mut env, &s)) {
        Ok(s) => s,
        Err(e) => {
            throw_msg(&mut env, &e);
            JString::default()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_github_balchua_datafusion_engine_NativeEngine_resultClose(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_id: jlong,
) {
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return;
        }
    };
    if let Err(e) = engine.close_result(query_id as u64) {
        throw(&mut env, &e);
    }
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
    let (_guard, engine) = match engine_from(handle) {
        Ok(pair) => pair,
        Err(e) => {
            throw(&mut env, &e);
            return JString::default();
        }
    };
    let sql = match get_string(&mut env, &sql) {
        Ok(s) => s,
        Err(e) => {
            throw_msg(&mut env, &e);
            return JString::default();
        }
    };
    let destination = match get_string(&mut env, &destination) {
        Ok(d) => d,
        Err(e) => {
            throw_msg(&mut env, &e);
            return JString::default();
        }
    };
    match engine.query_to_parquet(&sql, &destination) {
        Ok((encode_ns, publish_ns)) => {
            let json = format!(r#"{{"encode_nanos":{},"publish_nanos":{}}}"#, encode_ns, publish_ns);
            match new_string(&mut env, &json) {
                Ok(s) => s,
                Err(e) => {
                    throw_msg(&mut env, &e);
                    JString::default()
                }
            }
        }
        Err(e) => {
            throw(&mut env, &e);
            JString::default()
        }
    }
}
