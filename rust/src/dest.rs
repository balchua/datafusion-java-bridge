//! Destination classification shared by the engine (confinement check, object-store
//! resolution) and the Parquet writer, so the check and the actual write always agree on
//! where a destination points.

use std::path::PathBuf;
use url::Url;

use crate::error::{EngineError, ErrorKind, Result};

pub(crate) enum Destination {
    /// A local filesystem path (plain path, or a `file:` URL in any form).
    Local(PathBuf),
    /// A non-local URL (e.g. `s3://bucket/key`).
    Remote(Url),
}

/// Classifies a destination string.
///
/// A string is treated as a URL if it contains `://` **or** the URL parser accepts it as
/// `scheme:rest`. That closes the gap where `file:/x` (one slash) or `x:rest` would be
/// checked as one thing but written as another: `file:` is local, everything else is a
/// remote URL that the object-store registry then validates (an unsupported scheme or an
/// `s3:` URL without a bucket is rejected there). Anything that does not parse as a URL is
/// a plain local path.
pub(crate) fn classify(destination: &str) -> Result<Destination> {
    let is_url = destination.contains("://") || Url::parse(destination).is_ok();
    if !is_url {
        return Ok(Destination::Local(PathBuf::from(destination)));
    }
    let url = Url::parse(destination)
        .map_err(|e| EngineError::new(ErrorKind::ObjectStore, format!("bad destination: {e}")))?;
    if url.scheme() == "file" {
        let path = url.to_file_path().map_err(|_| {
            EngineError::new(
                ErrorKind::ObjectStore,
                format!("not a local file URL: {destination}"),
            )
        })?;
        Ok(Destination::Local(path))
    } else {
        Ok(Destination::Remote(url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local(s: &str) -> PathBuf {
        match classify(s).unwrap() {
            Destination::Local(p) => p,
            Destination::Remote(_) => panic!("expected local: {s}"),
        }
    }

    fn remote(s: &str) {
        assert!(
            matches!(classify(s).unwrap(), Destination::Remote(_)),
            "expected remote: {s}"
        );
    }

    #[test]
    fn classifies_destinations_consistently() {
        assert_eq!(local("a.parquet"), PathBuf::from("a.parquet"));
        assert_eq!(local("/abs/x.parquet"), PathBuf::from("/abs/x.parquet"));
        // `file:` in any form is local, and resolves to the real path.
        assert_eq!(
            local("file:///data/x.parquet"),
            PathBuf::from("/data/x.parquet")
        );
        assert_eq!(
            local("file:/data/x.parquet"),
            PathBuf::from("/data/x.parquet")
        );
        // Everything scheme-like but not `file:` is remote (and later rejected/validated).
        remote("s3://bucket/key");
        remote("s3:/bucket/key");
        remote("x:rest");
    }
}
