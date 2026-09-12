use std::fmt;

/// Error kinds exposed to Java. Each kind maps 1:1 to a Java exception class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidSql,
    UnsupportedSql,
    TableNotFound,
    SchemaMismatch,
    ArrowBridge,
    Parquet,
    ObjectStore,
    Resource,
    Cancelled,
    EngineClosed,
    Internal,
}

#[derive(Debug)]
pub struct EngineError {
    pub kind: ErrorKind,
    pub message: String,
}

impl EngineError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Fully qualified Java exception class for this kind.
    pub fn java_class(&self) -> &'static str {
        match self.kind {
            ErrorKind::InvalidSql => "io/github/balchua/datafusion/exception/SqlParseException",
            ErrorKind::UnsupportedSql => "io/github/balchua/datafusion/exception/SqlUnsupportedException",
            ErrorKind::TableNotFound => "io/github/balchua/datafusion/exception/TableNotFoundException",
            ErrorKind::SchemaMismatch => {
                "io/github/balchua/datafusion/exception/SchemaMismatchException"
            }
            ErrorKind::ArrowBridge => "io/github/balchua/datafusion/exception/ArrowBridgeException",
            ErrorKind::Parquet => "io/github/balchua/datafusion/exception/ParquetException",
            ErrorKind::ObjectStore => "io/github/balchua/datafusion/exception/ObjectStoreException",
            ErrorKind::Resource => "io/github/balchua/datafusion/exception/ResourceException",
            ErrorKind::Cancelled => "io/github/balchua/datafusion/exception/QueryCancelledException",
            ErrorKind::EngineClosed => "io/github/balchua/datafusion/exception/EngineClosedException",
            ErrorKind::Internal => "io/github/balchua/datafusion/exception/DataFusionException",
        }
    }
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for EngineError {}

impl From<datafusion::error::DataFusionError> for EngineError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        use datafusion::error::DataFusionError as E;
        let message = err.to_string();
        // Unwrap Context/Diagnostic wrappers so classification looks at the root cause.
        let kind = match err.find_root() {
            E::SQL(..) => {
                // DF 55 raises "table '...' not found" from SQL planning via the SQL variant.
                let lower = message.to_lowercase();
                if lower.contains("table '") && lower.contains("' not found") {
                    ErrorKind::TableNotFound
                } else {
                    ErrorKind::InvalidSql
                }
            }
            E::Plan(msg) => {
                let lower = msg.to_lowercase();
                if lower.contains("table '") && lower.contains("' not found") {
                    ErrorKind::TableNotFound
                } else if is_file_error(&lower) {
                    ErrorKind::Parquet
                } else {
                    ErrorKind::InvalidSql
                }
            }
            E::SchemaError(..) => ErrorKind::SchemaMismatch,
            E::NotImplemented(_) => ErrorKind::UnsupportedSql,
            E::ResourcesExhausted(_) => ErrorKind::Resource,
            E::ObjectStore(_) => ErrorKind::ObjectStore,
            E::ParquetError(_) => ErrorKind::Parquet,
            E::ArrowError(..) => ErrorKind::ArrowBridge,
            _ => ErrorKind::Internal,
        };
        Self { kind, message }
    }
}

/// Registration of a Parquet table surfaces missing/empty locations as Plan errors;
/// route them to ParquetException so the Java API stays honest about the cause.
fn is_file_error(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("no files found")
        || lower.contains("no such file")
        || lower.contains("does not exist")
        || lower.contains("not a directory")
        || lower.contains("filenotfound")
}

impl From<datafusion::arrow::error::ArrowError> for EngineError {
    fn from(err: datafusion::arrow::error::ArrowError) -> Self {
        Self::new(ErrorKind::ArrowBridge, err.to_string())
    }
}

impl From<object_store::Error> for EngineError {
    fn from(err: object_store::Error) -> Self {
        Self::new(ErrorKind::ObjectStore, err.to_string())
    }
}

impl From<datafusion::parquet::errors::ParquetError> for EngineError {
    fn from(err: datafusion::parquet::errors::ParquetError) -> Self {
        Self::new(ErrorKind::Parquet, err.to_string())
    }
}

impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        Self::new(ErrorKind::Resource, err.to_string())
    }
}

impl From<serde_json::Error> for EngineError {
    fn from(err: serde_json::Error) -> Self {
        Self::new(ErrorKind::Internal, format!("invalid engine config: {err}"))
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;