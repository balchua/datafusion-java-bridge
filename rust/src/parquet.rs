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

use crate::error::{EngineError, ErrorKind, Result};

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

/// Encodes a record batch stream to a scratch file. Returns the scratch path and the
/// time (ns) spent encoding + writing the local file.
///
/// Must be called from a blocking thread (it blocks on the async stream one batch at a
/// time via `block_on`; the writer itself is synchronous by design of the parquet crate).
pub fn encode_to_scratch(
    rt: &tokio::runtime::Handle,
    schema: SchemaRef,
    stream: datafusion::physical_plan::SendableRecordBatchStream,
    scratch_dir: &std::path::Path,
    compression: Compression,
) -> Result<(PathBuf, u64)> {
    std::fs::create_dir_all(scratch_dir)?;
    let scratch = scratch_dir.join(format!(
        "df_engine_{}.parquet",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    ));

    let props = WriterProperties::builder()
        .set_compression(compression)
        .build();

    let started = std::time::Instant::now();
    let file = std::fs::File::create(&scratch)?;
    let mut writer =
        datafusion::parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))?;
    let mut stream = stream;
    loop {
        match rt.block_on(stream.next()) {
            Some(Ok(batch)) => writer.write(&batch)?,
            Some(Err(e)) => return Err(e.into()),
            None => break,
        }
    }
    writer.close()?;

    Ok((scratch, started.elapsed().as_nanos() as u64))
}

/// Publishes the scratch file to the destination. `store` is `None` for local
/// filesystem destinations. Returns the publish time (ns). The scratch file is removed.
pub fn publish(
    rt: &tokio::runtime::Handle,
    scratch: &PathBuf,
    destination: &str,
    store: Option<Arc<dyn object_store::ObjectStore>>,
) -> Result<u64> {
    let started = std::time::Instant::now();
    match store {
        None => publish_local(scratch, destination)?,
        Some(store) => {
            let path = object_path(destination)?;
            rt.block_on(async move {
                let mut writer = BufWriter::with_capacity(store, path, 10 * 1024 * 1024);
                let mut file = tokio::fs::File::open(scratch).await?;
                // 8 MiB chunks: satisfies the 5 MiB minimum part size of S3 multipart.
                let mut buf = vec![0u8; 8 * 1024 * 1024];
                loop {
                    let n = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    tokio::io::AsyncWriteExt::write_all(&mut writer, &buf[..n]).await?;
                }
                tokio::io::AsyncWriteExt::flush(&mut writer).await?;
                tokio::io::AsyncWriteExt::shutdown(&mut writer).await?;
                Ok::<(), EngineError>(())
            })?;
        }
    }
    let _ = std::fs::remove_file(scratch);
    Ok(started.elapsed().as_nanos() as u64)
}

fn publish_local(scratch: &PathBuf, destination: &str) -> Result<()> {
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
    ObjectPath::from_url_path(key.to_string())
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
}