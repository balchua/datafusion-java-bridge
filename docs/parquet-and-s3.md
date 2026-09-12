# Parquet and S3

## Export (`queryToParquet`)

```java
engine.queryToParquet(sql, "s3://payments/out/txn.parquet");
engine.queryToParquet(sql, "/var/tmp/out/txn.parquet");   // or file:///…
```

Flow — the Java application never receives the result rows:

```text
SQL → sql_guard → DataFusion plan → execute stream
   → parquet::arrow::ArrowWriter (snappy by default, configurable)
   → scratch file in scratchDir            (encoding stage: fully written and closed)
   → publish:
        local path  → fs move/rename onto destination
        s3://…      → object_store multipart upload from the scratch file (I/O stage)
```

Rationale: encoding and network I/O are fused inside most writer stacks; staging to a local
scratch file first is what makes the two benchmark stages (encode vs I/O) separable and gives
atomic publication. The scratch file is deleted after publish. Cost: disk space ≈ result size,
one extra disk write for S3 destinations. Documented as a deliberate v1 trade-off; a fused
`DataFrame::write_parquet` path (single interleaved stage) can be added later if benchmarks
favor it.

Compression is set in `EngineConfig.parquetCompression`: `snappy` (default), `zstd`,
`uncompressed`.

## Import (`registerParquet`)

```java
engine.registerParquet("transactions", "/data/payments/txn.parquet");
engine.registerParquet("transactions", "s3://bucket/path/file.parquet");
```

DataFusion registers a listing table provider over the path. Parquet decoding happens inside
Rust/DataFusion with projection & predicate pushdown; Java never downloads or decodes bytes.
For `s3://` URLs the engine resolves (and caches) an `AmazonS3` client per bucket, built from
`EngineConfig.S3Config`, and registers it with the session's object-store registry.

## S3-compatible storage configuration

Works with AWS S3 and on-premises S3-compatible endpoints (MinIO, Ceph, NetApp StorageGRID,
etc.). Configuration is supplied by the application, never hard-coded:

```java
EngineConfig config = EngineConfig.builder()
    .memoryLimitBytes(4L * 1024 * 1024 * 1024)
    .s3(s3 -> s3
        .endpoint("http://minio.internal:9000")   // omit for AWS
        .region("us-east-1")
        .accessKey(System.getenv("S3_ACCESS_KEY"))
        .secretKey(System.getenv("S3_SECRET_KEY"))
        .allowHttp(true))                          // needed for plain-http on-prem endpoints
    .build();
```

Credentials may also come from the environment (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)
by leaving the config fields null. The bucket is taken from the URL (`s3://bucket/…`).
Path-style addressing (required by most on-prem endpoints) is enabled by the object_store
S3 builder semantics; virtual-host addressing is not required.

Behavior summary:

| URL | Store |
|---|---|
| `/path/file.parquet`, `file:///path/file.parquet` | DataFusion default `LocalFileSystem` |
| `s3://bucket/key` | `object_store::aws::AmazonS3` from config/env, cached per bucket |

Errors from parquet decode map to `ParquetException`; object-store/network errors map to
`ObjectStoreException`.