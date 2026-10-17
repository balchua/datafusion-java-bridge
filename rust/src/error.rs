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
    QueryNotFound,
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

    /// Error for an operation cancelled because the engine is shutting down.
    pub fn closing() -> Self {
        Self::new(ErrorKind::EngineClosed, "engine is closing")
    }

    /// Fully qualified Java exception class for this kind.
    pub fn java_class(&self) -> &'static str {
        match self.kind {
            ErrorKind::InvalidSql => "io/github/balchua/datafusion/exception/SqlParseException",
            ErrorKind::UnsupportedSql => {
                "io/github/balchua/datafusion/exception/SqlUnsupportedException"
            }
            ErrorKind::TableNotFound => {
                "io/github/balchua/datafusion/exception/TableNotFoundException"
            }
            ErrorKind::SchemaMismatch => {
                "io/github/balchua/datafusion/exception/SchemaMismatchException"
            }
            ErrorKind::ArrowBridge => "io/github/balchua/datafusion/exception/ArrowBridgeException",
            ErrorKind::Parquet => "io/github/balchua/datafusion/exception/ParquetException",
            ErrorKind::ObjectStore => "io/github/balchua/datafusion/exception/ObjectStoreException",
            ErrorKind::Resource => "io/github/balchua/datafusion/exception/ResourceException",
            ErrorKind::Cancelled => {
                "io/github/balchua/datafusion/exception/QueryCancelledException"
            }
            ErrorKind::EngineClosed => {
                "io/github/balchua/datafusion/exception/EngineClosedException"
            }
            ErrorKind::QueryNotFound => {
                "io/github/balchua/datafusion/exception/QueryNotFoundException"
            }
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
        let message = err.to_string();
        let kind = classify_error(&err, &message);
        Self { kind, message }
    }
}

/// Classifies a DataFusion error into the Java-visible [`ErrorKind`].
///
/// DataFusion does not expose a stable error taxonomy for planning failures, so the
/// Plan/SQL branches inspect the message text. The heuristics are factored into the
/// small helpers below and pinned by tests, so an upstream wording change on a
/// `cargo update` fails a test instead of silently reclassifying errors.
fn classify_error(err: &datafusion::error::DataFusionError, message: &str) -> ErrorKind {
    use datafusion::error::DataFusionError as E;
    // Unwrap Context/Diagnostic wrappers so classification looks at the root cause.
    match err.find_root() {
        E::SQL(..) => {
            if is_table_not_found(message) {
                ErrorKind::TableNotFound
            } else {
                ErrorKind::InvalidSql
            }
        }
        E::Plan(msg) => {
            if is_table_not_found(msg) {
                ErrorKind::TableNotFound
            } else if is_file_error(msg) {
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
    }
}

/// DataFusion raises "table '...' not found" from both SQL planning and the planner.
fn is_table_not_found(message: &str) -> bool {
    let lower = message.to_lowercase();
    lower.contains("table '") && lower.contains("' not found")
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
        // Never echo the raw serde message: it quotes the offending value, which may be a
        // credential. Only the location is safe to surface.
        Self::new(
            ErrorKind::Internal,
            format!(
                "invalid engine config JSON at line {} column {}",
                err.line(),
                err.column()
            ),
        )
    }
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::error::DataFusionError;

    fn kind_of(err: DataFusionError) -> ErrorKind {
        EngineError::from(err).kind
    }

    #[test]
    fn table_not_found_helper() {
        assert!(is_table_not_found("table 'foo' not found"));
        assert!(is_table_not_found("Error: Table 'Foo' NOT FOUND"));
        assert!(!is_table_not_found("table 'foo' has no column bar"));
        assert!(!is_table_not_found("syntax error"));
    }

    #[test]
    fn file_error_helper() {
        assert!(is_file_error("No files found at file:///nope"));
        assert!(is_file_error("No such file or directory"));
        assert!(is_file_error("path does not exist"));
        assert!(!is_file_error("syntax error near FROM"));
    }

    #[test]
    fn plan_variants_classify() {
        // Pin the message heuristics so an upstream DataFusion wording change fails a test.
        assert_eq!(
            kind_of(DataFusionError::Plan("table 'p' not found".into())),
            ErrorKind::TableNotFound
        );
        assert_eq!(
            kind_of(DataFusionError::Plan(
                "No files found at file:///tmp/x".into()
            )),
            ErrorKind::Parquet
        );
        assert_eq!(
            kind_of(DataFusionError::Plan("generic planning failure".into())),
            ErrorKind::InvalidSql
        );
    }

    #[test]
    fn sql_variant_classifies_table_not_found() {
        use datafusion::sql::sqlparser::parser::ParserError;
        let sql_err = |msg: &str| {
            DataFusionError::SQL(Box::new(ParserError::ParserError(msg.to_string())), None)
        };
        assert_eq!(
            kind_of(sql_err("table 'p' not found")),
            ErrorKind::TableNotFound
        );
        assert_eq!(
            kind_of(sql_err("Expected an SQL statement, found: SELEC")),
            ErrorKind::InvalidSql
        );
    }

    #[test]
    fn simple_variants_classify() {
        assert_eq!(
            kind_of(DataFusionError::NotImplemented("nope".into())),
            ErrorKind::UnsupportedSql
        );
        assert_eq!(
            kind_of(DataFusionError::ResourcesExhausted("out of memory".into())),
            ErrorKind::Resource
        );
    }

    #[test]
    fn query_not_found_maps_to_its_own_java_exception() {
        let e = EngineError::new(ErrorKind::QueryNotFound, "query 1 is not registered");
        assert_eq!(
            e.java_class(),
            "io/github/balchua/datafusion/exception/QueryNotFoundException"
        );
        assert!(e.to_string().contains("not registered"));
    }
}
