//! Parquet export: encode a query result into a scratch Parquet file, then publish
//! it to a local path or an S3-compatible bucket.
//!
//! Encoding and network I/O are separated on purpose: encoding is measured while the
//! scratch file is written, publishing (rename or multipart upload) is measured
//! separately. See docs/parquet-and-s3.md.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::error::{EngineError, ErrorKind, Result};

/// Bytes read per chunk while uploading (>= the 5 MiB S3 multipart minimum).
const UPLOAD_CHUNK: usize = 8 * 1024 * 1024;
/// Capacity of the object-store `BufWriter` buffer.
const MULTIPART_CHUNK: usize = 10 * 1024 * 1024;
/// Upper bound on how long to wait for a best-effort multipart abort.
const ABORT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Awaits `fut`, giving up with a shutdown error if the engine cancels first.
async fn cancellable<T>(
    cancel: &CancellationToken,
    fut: impl std::future::Future<Output = std::io::Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(EngineError::closing()),
        r = fut => r.map_err(EngineError::from),
    }
}

pub fn compression_from_name(name: &str) -> Result<Compression> {
    match name.to_ascii_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        other => Err(EngineError::new(
            ErrorKind::Parquet,
            format!("unsupported compression '{other}' (supported: snappy, zstd, uncompressed)"),
        )),
    }
}

/// Owns the scratch Parquet file and deletes it on drop, so a cancelled or failed export
/// never leaves a partial file behind — even when the operation future is dropped mid-await
/// by the engine-level cancellation `select!`.
pub struct ScratchFile(PathBuf);

impl ScratchFile {
    pub fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for ScratchFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Creates a fresh scratch file with `O_EXCL` (`create_new`) and owner-only permissions.
///
/// The name mixes the pid, a nanosecond timestamp and a process-wide counter so concurrent
/// engines/threads do not collide; `create_new` refuses to follow or clobber an existing
/// path (closing the symlink/clobber vector), and the file is created `0600` on Unix so
/// query results are not world-readable in a shared temp directory.
fn create_scratch_file(dir: &std::path::Path) -> Result<(PathBuf, std::fs::File)> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    std::fs::create_dir_all(dir)?;
    for _ in 0..64 {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = dir.join(format!(
            "df_engine_{}_{}_{}.parquet",
            std::process::id(),
            nanos,
            n
        ));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        match opts.open(&path) {
            Ok(f) => return Ok((path, f)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(EngineError::new(
        ErrorKind::Resource,
        "could not create a unique scratch file",
    ))
}

/// Encodes a record batch stream to a scratch file. Returns the scratch file and the
/// time (ns) spent encoding + writing it.
///
/// Checks `cancel` before each batch; a single `ArrowWriter::write` call is synchronous and
/// cannot be interrupted mid-batch, which bounds cancellation latency to one batch. The
/// returned [`ScratchFile`] removes the file on drop.
pub async fn encode_to_scratch(
    schema: SchemaRef,
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    scratch_dir: &std::path::Path,
    compression: Compression,
    cancel: &CancellationToken,
) -> Result<(ScratchFile, u64)> {
    let (path, file) = create_scratch_file(scratch_dir)?;
    let scratch = ScratchFile(path);

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let started = std::time::Instant::now();
    let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))?;
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(EngineError::closing()),
            next = stream.next() => next,
        };
        match next {
            Some(Ok(batch)) => writer.write(&batch)?,
            Some(Err(e)) => return Err(e.into()),
            None => break,
        }
    }
    writer.close()?;

    Ok((scratch, started.elapsed().as_nanos() as u64))
}

/// Publishes the scratch file to the destination. `store` is `None` for local
/// filesystem destinations. Returns the publish time (ns). The caller owns the
/// [`ScratchFile`], which removes it on drop.
///
/// For object stores the upload is cancelled at every network await (chunk read/write,
/// flush, shutdown); on cancellation the multipart upload is aborted (best effort, with a
/// bounded timeout so an unresponsive endpoint cannot block the caller).
pub async fn publish(
    scratch: &std::path::Path,
    destination: &str,
    store: Option<Arc<dyn object_store::ObjectStore>>,
    cancel: &CancellationToken,
    overwrite: bool,
) -> Result<u64> {
    let started = std::time::Instant::now();
    match store {
        None => publish_local(scratch, destination, overwrite)?,
        Some(store) => {
            let path = object_path(destination)?;
            let mut writer = BufWriter::with_capacity(store, path, MULTIPART_CHUNK);
            let mut file = tokio::fs::File::open(scratch).await?;
            // UPLOAD_CHUNK satisfies the 5 MiB minimum part size of S3 multipart.
            let mut buf = vec![0u8; UPLOAD_CHUNK];
            loop {
                let n =
                    match cancellable(cancel, tokio::io::AsyncReadExt::read(&mut file, &mut buf))
                        .await
                    {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) => {
                            abort_upload(&mut writer).await;
                            return Err(e);
                        }
                    };
                if let Err(e) = cancellable(
                    cancel,
                    tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n]),
                )
                .await
                {
                    abort_upload(&mut writer).await;
                    return Err(e);
                }
            }
            // Once flush/shutdown has begun, the BufWriter is in its `Flush` state where
            // `abort()` panics ("Already shut down"), so we must NOT abort here — just
            // surface the cancellation/error. Aborting is only safe before flush starts
            // (the read/write loop above).
            cancellable(cancel, tokio::io::AsyncWriteExt::flush(&mut writer)).await?;
            cancellable(cancel, tokio::io::AsyncWriteExt::shutdown(&mut writer)).await?;
        }
    }
    Ok(started.elapsed().as_nanos() as u64)
}

/// Best-effort abort of an in-flight multipart upload, bounded so an unresponsive endpoint
/// cannot block cancellation or a failing publish indefinitely. If the timeout fires the
/// upload is simply left to the store's own lifecycle rules.
async fn abort_upload(writer: &mut BufWriter) {
    let _ = tokio::time::timeout(ABORT_TIMEOUT, writer.abort()).await;
}

fn publish_local(scratch: &std::path::Path, destination: &str, overwrite: bool) -> Result<()> {
    let dest = local_path(destination)?;
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    if !overwrite && dest.exists() {
        // Refuse to clobber. (Also enforced atomically below via hard_link / create_new.)
        return Err(EngineError::new(
            ErrorKind::Parquet,
            format!(
                "destination already exists (overwrite disabled): {}",
                dest.display()
            ),
        ));
    }
    // rename is atomic on the same filesystem; for a cross-device move (EXDEV) we fall
    // back to copy-then-remove, which is NOT atomic: a crash between the two leaves a
    // complete destination file behind (the scratch file is removed by `ScratchFile::drop`).
    // When overwriting is disabled, `rename` would clobber, so use a no-clobber link/copy.
    if overwrite {
        match std::fs::rename(scratch, &dest) {
            Ok(()) => {}
            // Cross-device move only: copy. Any other error is propagated (not masked).
            Err(e) if e.kind() == std::io::ErrorKind::CrossesDevices => {
                std::fs::copy(scratch, &dest)?;
                let _ = std::fs::remove_file(scratch);
            }
            Err(e) => return Err(e.into()),
        }
    } else {
        publish_no_clobber(scratch, &dest)?;
    }
    Ok(())
}

/// Publishes without overwriting an existing destination.
///
/// Atomic where the filesystem supports a hard link; otherwise the file is copied directly
/// into a freshly created destination (`O_EXCL`, still no-clobber, though not atomic).
fn publish_no_clobber(scratch: &std::path::Path, dest: &std::path::Path) -> Result<()> {
    match std::fs::hard_link(scratch, dest) {
        Ok(()) => Ok(()), // scratch removed by `ScratchFile::drop`
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(dest_exists(dest)),
        // Cross-device, or a filesystem without hard-link support (FAT/SMB/FUSE/...):
        // copy straight into the destination with `create_new`.
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::CrossesDevices
                    | std::io::ErrorKind::Unsupported
                    | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            let mut opts = std::fs::OpenOptions::new();
            opts.write(true).create_new(true);
            let mut out = opts.open(dest).map_err(|e| {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    dest_exists(dest)
                } else {
                    e.into()
                }
            })?;
            match std::fs::File::open(scratch).and_then(|mut src| std::io::copy(&mut src, &mut out))
            {
                Ok(_) => {
                    let _ = std::fs::remove_file(scratch);
                    Ok(())
                }
                Err(err) => {
                    // Do not leave a truncated destination behind.
                    let _ = std::fs::remove_file(dest);
                    Err(err.into())
                }
            }
        }
        // Any other error (e.g. EACCES, ELOOP) is propagated, not silently masked.
        Err(e) => Err(e.into()),
    }
}

fn dest_exists(dest: &std::path::Path) -> EngineError {
    EngineError::new(
        ErrorKind::Parquet,
        format!(
            "destination already exists (overwrite disabled): {}",
            dest.display()
        ),
    )
}

/// Resolves a local filesystem destination to a path, using the shared classifier so it
/// agrees with `resolve_store`/`enforce_allowed_root` (a `file:` scheme is stripped in every
/// form; a remote URL is rejected here).
fn local_path(destination: &str) -> Result<PathBuf> {
    match crate::dest::classify(destination)? {
        crate::dest::Destination::Local(p) => Ok(p),
        crate::dest::Destination::Remote(_) => Err(EngineError::new(
            ErrorKind::ObjectStore,
            format!("expected a local destination: {destination}"),
        )),
    }
}

/// Object key relative to the bucket for `s3://bucket/key/...` destinations.
fn object_path(destination: &str) -> Result<ObjectPath> {
    let parsed = url::Url::parse(destination).map_err(|e| {
        EngineError::new(ErrorKind::ObjectStore, format!("bad destination URL: {e}"))
    })?;
    let key = parsed.path().trim_start_matches('/');
    ObjectPath::from_url_path(key)
        .map_err(|e| EngineError::new(ErrorKind::ObjectStore, format!("bad object path: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_names() {
        assert!(compression_from_name("snappy").is_ok());
        assert!(compression_from_name("ZSTD").is_ok());
        assert!(compression_from_name("uncompressed").is_ok());
        assert!(compression_from_name("brotli").is_err());
    }

    #[test]
    fn local_path_strips_file_scheme() {
        assert_eq!(
            local_path("file:///data/x.parquet").unwrap(),
            PathBuf::from("/data/x.parquet")
        );
        assert_eq!(
            local_path("/data/y.parquet").unwrap(),
            PathBuf::from("/data/y.parquet")
        );
    }

    #[test]
    fn publish_no_clobber_refuses_existing_destination() {
        let dir = std::env::temp_dir().join(format!("dfe-no-clobber-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let scratch = dir.join("scratch.bin");
        let dest = dir.join("dest.bin");
        std::fs::write(&scratch, b"data").unwrap();
        std::fs::write(&dest, b"existing").unwrap();

        let err = publish_no_clobber(&scratch, &dest).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Parquet);
        assert_eq!(
            std::fs::read(&dest).unwrap(),
            b"existing",
            "existing file must be untouched"
        );

        let _ = std::fs::remove_file(&scratch);
        let _ = std::fs::remove_file(&dest);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn encode_is_cancelled_promptly() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::time::{Duration, Instant};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        // A stream that never yields a batch: without cooperative cancellation this
        // encode would block forever. The Item type is spelled out so inference can't
        // produce an ambiguous `Pending<_>`.
        let pending: futures::stream::Pending<
            std::result::Result<
                datafusion::arrow::array::RecordBatch,
                datafusion::error::DataFusionError,
            >,
        > = futures::stream::pending();
        let stream: datafusion::physical_plan::SendableRecordBatchStream = Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema.clone(),
                pending,
            ),
        );
        let cancel = CancellationToken::new();
        let from_thread = cancel.clone();
        let canceller = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            from_thread.cancel();
        });

        let started = Instant::now();
        let result = rt.block_on(encode_to_scratch(
            schema,
            stream,
            &std::env::temp_dir(),
            Compression::SNAPPY,
            &cancel,
        ));
        canceller.join().unwrap();

        assert!(matches!(result, Err(ref e) if e.kind == ErrorKind::EngineClosed));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "cancellation should be prompt, took {:?}",
            started.elapsed()
        );
    }
}
