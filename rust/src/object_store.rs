//! Object-store construction (filesystem + S3-compatible) and DataFusion registration.

use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::error::{EngineError, ErrorKind, Result};

/// S3-compatible storage configuration. Credentials are supplied by the application or
/// taken from the environment — never hard-coded.
#[derive(Debug, Clone, serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    /// Custom endpoint for on-premises S3-compatible storage (MinIO, Ceph, ...).
    /// Omit for AWS. Example: "http://minio.internal:9000"
    #[serde(default)]
    pub endpoint: Option<String>,
    /// AWS region (default us-east-1).
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    /// Required for plain-http endpoints (on-premises).
    #[serde(default)]
    pub allow_http: bool,
}

/// Builds an S3-compatible object store for a bucket.
///
/// Falls back to the standard AWS credential chain (environment/IMDS) when
/// access_key/secret_key are not configured.
pub fn build_s3_store(config: &S3Config, bucket: &str) -> Result<Arc<dyn object_store::ObjectStore>> {
    let mut builder = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(bucket.to_string())
        .with_allow_http(config.allow_http);

    if let Some(endpoint) = &config.endpoint {
        builder = builder.with_endpoint(endpoint.clone());
    }
    if let Some(region) = &config.region {
        builder = builder.with_region(region.clone());
    } else {
        builder = builder.with_region("us-east-1".to_string());
    }
    match (&config.access_key, &config.secret_key) {
        (Some(key), Some(secret)) => {
            builder = builder
                .with_access_key_id(key.clone())
                .with_secret_access_key(secret.clone());
        }
        (None, None) => {
            // leave credentials to the default chain (env vars / instance metadata)
        }
        _ => {
            return Err(EngineError::new(
                ErrorKind::ObjectStore,
                "s3 config requires both access_key and secret_key (or neither, to use the AWS default credential chain)",
            ));
        }
    }

    let store = builder
        .build()
        .map_err(|e| EngineError::new(ErrorKind::ObjectStore, format!("s3 client build failed: {e}")))?;
    Ok(Arc::new(store))
}

/// Registry that resolves `s3://bucket/...` URLs to per-bucket stores built from
/// the engine configuration. Local paths and `file://` URLs are handled by
/// DataFusion's built-in LocalFileSystem and need no entry here.
#[derive(Default)]
pub struct ObjectStoreRegistry {
    s3: Option<S3Config>,
    stores: HashMap<String, Arc<dyn object_store::ObjectStore>>,
}

impl ObjectStoreRegistry {
    pub fn new(s3: Option<S3Config>) -> Self {
        Self {
            s3,
            stores: HashMap::new(),
        }
    }

    /// Returns (and caches) a store for the given URL's scheme+authority, or `None`
    /// when the URL is handled by DataFusion defaults (local filesystem).
    pub fn store_for(&mut self, url: &Url) -> Result<Option<(Url, Arc<dyn object_store::ObjectStore>)>> {
        match url.scheme() {
            "file" | "" => Ok(None),
            "s3" => {
                let bucket = url.host_str().ok_or_else(|| {
                    EngineError::new(
                        ErrorKind::ObjectStore,
                        format!("s3 URL without bucket: {url}"),
                    )
                })?;
                if let Some(existing) = self.stores.get(bucket) {
                    let store_url = Url::parse(&format!("s3://{bucket}")).map_err(|e| {
                        EngineError::new(ErrorKind::ObjectStore, format!("bad s3 url: {e}"))
                    })?;
                    return Ok(Some((store_url, existing.clone())));
                }
                let config = self.s3.as_ref().ok_or_else(|| {
                    EngineError::new(
                        ErrorKind::ObjectStore,
                        "s3:// URL used but no S3 configuration was provided to the engine",
                    )
                })?;
                let store = build_s3_store(config, bucket)?;
                self.stores.insert(bucket.to_string(), store.clone());
                let store_url = Url::parse(&format!("s3://{bucket}")).map_err(|e| {
                    EngineError::new(ErrorKind::ObjectStore, format!("bad s3 url: {e}"))
                })?;
                Ok(Some((store_url, store)))
            }
            other => Err(EngineError::new(
                ErrorKind::ObjectStore,
                format!("unsupported URL scheme '{other}' in: {url}"),
            )),
        }
    }
}