//! Parquet export: encode a query result into a scratch Parquet file, then publish
//! it to a local path or an S3-compatible bucket.
//!
//! Encoding and network I/O are separated on purpose: encoding is measured while the
//! scratch file is written, publishing (rename or multipart upload) is measured
//! separately. See docs/parquet-and-s3.md.

use datafusion::arrow::datatypes::SchemaRef;
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path as ObjectPath;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::error::{EngineError, ErrorKind, Result};

/// Error returned when the token fires because the engine is shutting down.
fn cancelled() -> EngineError {
    EngineError::new(ErrorKind::EngineClosed, "engine is closing")
}

pub fn compression_from_name(name: &str) -> Result<Compression> {
    match name.to_ascii_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "zstd" => Ok(Compression::ZSTD(Default::default())),
        "uncompressed" | "none" => Ok(Compression::UNCOMPRESSED),
        other => Err(EngineError::new(
            ErrorKind::Parquet,
            format!(
                "unsupported compression '{other}' (supported: snappy, zstd, uncompressed)"
            ),
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
    std::fs::create_dir_all(scratch_dir)?;
    let scratch = ScratchFile(scratch_dir.join(format!(
        "df_engine_{}.parquet",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    )));

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let started = std::time::Instant::now();
    let file = std::fs::File::create(scratch.path())?;
    let mut writer =
        datafusion::parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))?;
    loop {
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(cancelled()),
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
/// For object stores the upload is cancelled at chunk boundaries; on cancellation the
/// multipart upload is aborted (best effort).
pub async fn publish(
    scratch: &std::path::Path,
    destination: &str,
    store: Option<Arc<dyn object_store::ObjectStore>>,
    cancel: &CancellationToken,
) -> Result<u64> {
    let started = std::time::Instant::now();
    match store {
        None => publish_local(scratch, destination)?,
        Some(store) => {
            let path = object_path(destination)?;
            let mut writer = BufWriter::with_capacity(store, path, 10 * 1024 * 1024);
            let mut file = tokio::fs::File::open(scratch).await?;
            // 8 MiB chunks: satisfies the 5 MiB minimum part size of S3 multipart.
            let mut buf = vec![0u8; 8 * 1024 * 1024];
            loop {
                let read = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => Err(cancelled()),
                    r = tokio::io::AsyncReadExt::read(&mut file, &mut buf) => r.map_err(EngineError::from),
                };
                let n = match read {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => {
                        let _ = writer.abort().await;
                        return Err(e);
                    }
                };
                let written = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => Err(cancelled()),
                    r = tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n]) => r.map_err(EngineError::from),
                };
                if let Err(e) = written {
                    let _ = writer.abort().await;
                    return Err(e);
                }
            }
            tokio::io::AsyncWriteExt::flush(&mut writer).await?;
            tokio::io::AsyncWriteExt::shutdown(&mut writer).await?;
        }
    }
    Ok(started.elapsed().as_nanos() as u64)
}

fn publish_local(scratch: &std::path::Path, destination: &str) -> Result<()> {
    let dest = PathBuf::from(destination);
    if let Some(parent) = dest.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    // rename is atomic on the same filesystem; fall back to copy for cross-device moves.
    if std::fs::rename(scratch, &dest).is_err() {
        std::fs::copy(scratch, &dest)?;
        let _ = std::fs::remove_file(scratch);
    }
    Ok(())
}

/// Object key relative to the bucket for `s3://bucket/key/...` destinations.
fn object_path(destination: &str) -> Result<ObjectPath> {
    let parsed = url::Url::parse(destination)
        .map_err(|e| EngineError::new(ErrorKind::ObjectStore, format!("bad destination URL: {e}")))?;
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
        let stream: datafusion::physical_plan::SendableRecordBatchStream =
            Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                schema.clone(),
                pending,
            ));
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