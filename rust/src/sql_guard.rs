use crate::error::{EngineError, ErrorKind, Result};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;

/// Validates that SQL is a single, plain SELECT query.
///
/// Uses DataFusion's own bundled SQL parser (sqlparser + DFParser). Anything that is not
/// a plain `SELECT` (DDL, DML, COPY, CREATE EXTERNAL TABLE, EXPLAIN, RESET, multi-statement
/// strings) is rejected before DataFusion ever sees the text.
///
/// `SQLOptions` (ddl/dml/statement guards) is applied by DataFusion itself as a second line of
/// defense at planning time; this module gives precise, early error messages.
pub fn validate_select_only(sql: &str) -> Result<()> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(EngineError::new(ErrorKind::InvalidSql, "empty SQL statement"));
    }

    let statements = DFParser::parse_sql(trimmed)
        .map_err(|e| EngineError::new(ErrorKind::InvalidSql, format!("invalid SQL: {e}")))?;

    if statements.len() != 1 {
        return Err(EngineError::new(
            ErrorKind::UnsupportedSql,
            format!(
                "expected exactly one SELECT statement, found {} statements",
                statements.len()
            ),
        ));
    }

    match statements.front() {
        Some(DFStatement::Statement(inner)) => match **inner {
            SqlStatement::Query(_) => Ok(()),
            ref other => Err(EngineError::new(
                ErrorKind::UnsupportedSql,
                format!("only SELECT queries are allowed, got: {other:?}"),
            )),
        },
        Some(other) => Err(EngineError::new(
            ErrorKind::UnsupportedSql,
            format!("only SELECT queries are allowed, got: {other:?}"),
        )),
        None => Err(EngineError::new(ErrorKind::InvalidSql, "empty SQL statement")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_plain_select() {
        assert!(validate_select_only("SELECT 1").is_ok());
        assert!(validate_select_only("select a, b FROM t WHERE a > 1 GROUP BY b ORDER BY a LIMIT 5").is_ok());
        assert!(validate_select_only("SELECT count(*) FROM t JOIN u ON t.id = u.id").is_ok());
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_select_only("").is_err());
        assert!(validate_select_only("   ").is_err());
    }

    #[test]
    fn rejects_dml() {
        assert!(validate_select_only("INSERT INTO t VALUES (1)").is_err());
        assert!(validate_select_only("UPDATE t SET a = 1").is_err());
        assert!(validate_select_only("DELETE FROM t").is_err());
    }

    #[test]
    fn rejects_ddl() {
        assert!(validate_select_only("CREATE TABLE t (a INT)").is_err());
        assert!(validate_select_only("DROP TABLE t").is_err());
        assert!(validate_select_only("CREATE VIEW v AS SELECT 1").is_err());
        assert!(validate_select_only("ALTER TABLE t ADD COLUMN b INT").is_err());
    }

    #[test]
    fn rejects_copy_and_set_and_multi() {
        assert!(validate_select_only("COPY t TO 'out.parquet'").is_err());
        assert!(validate_select_only("SET datafusion.execution.batch_size = 100").is_err());
        assert!(validate_select_only("SELECT 1; SELECT 2").is_err());
        assert!(validate_select_only("SELECT 1; DROP TABLE t").is_err());
    }

    #[test]
    fn rejects_invalid_sql() {
        assert!(validate_select_only("SELEC 1").is_err());
        assert!(validate_select_only("SELECT 1 FROM").is_err());
    }
}