//! Object-store construction (filesystem + S3-compatible) and DataFusion registration.

use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

use crate::error::{EngineError, ErrorKind, Result};

/// S3-compatible storage configuration. Credentials are supplied by the application or
/// taken from the environment — never hard-coded.
#[derive(Clone, serde::Deserialize, Default)]
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
    /// Buckets callers may target. Empty (default) = any bucket the credentials can reach.
    /// When set, a URL naming any other bucket is rejected.
    #[serde(default)]
    pub allowed_buckets: Vec<String>,
}

/// Manual `Debug` that redacts credentials: a stray `{:?}` of the config (e.g. while
/// diagnosing an engine-create failure) must never print the access/secret keys.
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field(
                "access_key",
                &self.access_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "secret_key",
                &self.secret_key.as_ref().map(|_| "<redacted>"),
            )
            .field("allow_http", &self.allow_http)
            .field("allowed_buckets", &self.allowed_buckets)
            .finish()
    }
}

/// Builds an S3-compatible object store for a bucket.
///
/// Falls back to the standard AWS credential chain (environment/IMDS) when
/// access_key/secret_key are not configured.
pub fn build_s3_store(
    config: &S3Config,
    bucket: &str,
) -> Result<Arc<dyn object_store::ObjectStore>> {
    // `from_env` reads the standard AWS_* environment variables (access key, secret,
    // region, endpoint, ...) so credentials may come from the environment as documented;
    // the explicit config below overrides them.
    let mut builder = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(bucket.to_string())
        .with_allow_http(config.allow_http);

    if let Some(endpoint) = &config.endpoint {
        if endpoint.contains('@') {
            // Reject userinfo: it would leak (and log) credentials embedded in the URL.
            return Err(EngineError::new(
                ErrorKind::ObjectStore,
                "s3 endpoint must not contain credentials (user:pass@host)",
            ));
        }
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

    let store = builder.build().map_err(|e| {
        EngineError::new(
            ErrorKind::ObjectStore,
            format!("s3 client build failed: {e}"),
        )
    })?;
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

/// Upper bound on cached per-bucket stores, so cycling bucket names cannot grow the cache
/// (and its HTTP client pools) without limit.
const MAX_CACHED_STORES: usize = 128;

/// Canonical `s3://bucket` URL used to register the store with DataFusion.
fn bucket_url(bucket: &str) -> Result<Url> {
    Url::parse(&format!("s3://{bucket}"))
        .map_err(|e| EngineError::new(ErrorKind::ObjectStore, format!("bad s3 url: {e}")))
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
    pub fn store_for(
        &mut self,
        url: &Url,
    ) -> Result<Option<(Url, Arc<dyn object_store::ObjectStore>)>> {
        match url.scheme() {
            "file" | "" => Ok(None),
            "s3" => {
                let bucket = url.host_str().ok_or_else(|| {
                    EngineError::new(
                        ErrorKind::ObjectStore,
                        format!("s3 URL without bucket: {url}"),
                    )
                })?;
                // Confinement: when an allow-list is configured, reject any other bucket.
                if let Some(cfg) = &self.s3 {
                    if !cfg.allowed_buckets.is_empty()
                        && !cfg.allowed_buckets.iter().any(|b| b == bucket)
                    {
                        return Err(EngineError::new(
                            ErrorKind::ObjectStore,
                            format!("bucket '{bucket}' is not in the configured allowed list"),
                        ));
                    }
                }
                if let Some(existing) = self.stores.get(bucket) {
                    return Ok(Some((bucket_url(bucket)?, existing.clone())));
                }
                if self.stores.len() >= MAX_CACHED_STORES {
                    return Err(EngineError::new(
                        ErrorKind::ObjectStore,
                        "too many distinct object-store buckets have been used",
                    ));
                }
                let config = self.s3.as_ref().ok_or_else(|| {
                    EngineError::new(
                        ErrorKind::ObjectStore,
                        "s3:// URL used but no S3 configuration was provided to the engine",
                    )
                })?;
                let store = build_s3_store(config, bucket)?;
                self.stores.insert(bucket.to_string(), store.clone());
                Ok(Some((bucket_url(bucket)?, store)))
            }
            other => Err(EngineError::new(
                ErrorKind::ObjectStore,
                format!("unsupported URL scheme '{other}' in: {url}"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3_config() -> S3Config {
        S3Config {
            endpoint: Some("http://localhost:9000".to_string()),
            region: Some("us-east-1".to_string()),
            access_key: Some("key".to_string()),
            secret_key: Some("secret".to_string()),
            allow_http: true,
            allowed_buckets: Vec::new(),
        }
    }

    #[test]
    fn local_and_file_urls_need_no_store() {
        let mut reg = ObjectStoreRegistry::new(None);
        assert!(reg
            .store_for(&Url::parse("file:///tmp/x.parquet").unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn s3_without_config_is_rejected() {
        let mut reg = ObjectStoreRegistry::new(None);
        let err = reg
            .store_for(&Url::parse("s3://bucket/key.parquet").unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::ObjectStore);
    }

    #[test]
    fn s3_store_is_built_and_cached_per_bucket() {
        let mut reg = ObjectStoreRegistry::new(Some(s3_config()));
        let (url1, store1) = reg
            .store_for(&Url::parse("s3://bucket/a.parquet").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(url1.as_str(), "s3://bucket");
        // Second lookup for the same bucket returns the cached instance (same Arc).
        let (_, store2) = reg
            .store_for(&Url::parse("s3://bucket/b.parquet").unwrap())
            .unwrap()
            .unwrap();
        assert!(
            Arc::ptr_eq(&store1, &store2),
            "store should be cached per bucket"
        );
        // A different bucket yields a different store.
        let (_, store3) = reg
            .store_for(&Url::parse("s3://other/c.parquet").unwrap())
            .unwrap()
            .unwrap();
        assert!(!Arc::ptr_eq(&store1, &store3));
    }

    #[test]
    fn s3_allowed_buckets_confines_targets() {
        let mut cfg = s3_config();
        cfg.allowed_buckets = vec!["allowed".to_string()];
        let mut reg = ObjectStoreRegistry::new(Some(cfg));
        assert!(reg
            .store_for(&Url::parse("s3://allowed/a.parquet").unwrap())
            .is_ok());
        let err = reg
            .store_for(&Url::parse("s3://denied/a.parquet").unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::ObjectStore);
        assert!(err.message.contains("allowed list"));
    }

    #[test]
    fn unsupported_scheme_is_rejected() {
        let mut reg = ObjectStoreRegistry::new(None);
        let err = reg
            .store_for(&Url::parse("gs://bucket/key").unwrap())
            .unwrap_err();
        assert_eq!(err.kind, ErrorKind::ObjectStore);
        assert!(err.message.contains("unsupported URL scheme"));
    }

    #[test]
    fn s3_requires_both_credentials_or_neither() {
        let mut partial = s3_config();
        partial.secret_key = None;
        assert_eq!(
            build_s3_store(&partial, "bucket").unwrap_err().kind,
            ErrorKind::ObjectStore
        );
    }
}
