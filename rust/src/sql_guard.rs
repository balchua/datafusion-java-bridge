use crate::error::{EngineError, ErrorKind, Result};
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Query, SetExpr, Statement as SqlStatement};

/// Maximum accepted SQL length in bytes. Bounds parser/planner work, expression depth, and
/// the cost of the (recursive, ~quadratic) planning step.
pub const MAX_SQL_BYTES: usize = 16 * 1024;

/// Validates that SQL is a single, plain SELECT query.
///
/// Uses DataFusion's own bundled SQL parser (sqlparser + DFParser). Anything that is not
/// a plain `SELECT` (DDL, DML, COPY, CREATE EXTERNAL TABLE, EXPLAIN, RESET, multi-statement
/// strings, `SELECT ... INTO`) is rejected before DataFusion ever sees the text.
///
/// `SQLOptions` (ddl/dml/statement guards) is applied by DataFusion itself as a second line of
/// defense at planning time; this module gives precise, early error messages.
pub fn validate_select_only(sql: &str) -> Result<()> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Err(EngineError::new(
            ErrorKind::InvalidSql,
            "empty SQL statement",
        ));
    }
    // Bound the input before parsing: a very long statement is both a DoS vector and a
    // way to build expression trees deep enough to overflow the parser's/planner's stack.
    // (Planning also runs on a large-stack thread; see `Engine::plan_with_large_stack`.)
    if trimmed.len() > MAX_SQL_BYTES {
        return Err(EngineError::new(
            ErrorKind::InvalidSql,
            format!(
                "SQL statement too long: {} bytes (max {})",
                trimmed.len(),
                MAX_SQL_BYTES
            ),
        ));
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
            SqlStatement::Query(ref query) => {
                if query_has_into(query) {
                    // `SELECT ... INTO new_table` parses as a Query/Select but creates a
                    // table, so it must not pass the read-only guard.
                    Err(EngineError::new(
                        ErrorKind::UnsupportedSql,
                        "SELECT ... INTO is not allowed; only read-only queries are permitted",
                    ))
                } else {
                    Ok(())
                }
            }
            _ => Err(EngineError::new(
                ErrorKind::UnsupportedSql,
                "only read-only SELECT queries are allowed",
            )),
        },
        Some(_) => Err(EngineError::new(
            ErrorKind::UnsupportedSql,
            "only read-only SELECT queries are allowed",
        )),
        None => Err(EngineError::new(
            ErrorKind::InvalidSql,
            "empty SQL statement",
        )),
    }
}

/// Whether a query (including its CTE definitions and every UNION/EXCEPT/INTERSECT
/// branch) contains a `SELECT ... INTO`. CTEs are traversed so `SELECT ... INTO` cannot
/// hide inside a `WITH` clause.
fn query_has_into(query: &Query) -> bool {
    if let Some(with) = &query.with {
        if with.cte_tables.iter().any(|cte| query_has_into(&cte.query)) {
            return true;
        }
    }
    select_into_present(&query.body)
}

/// Whether any SELECT in this set expression carries an `INTO` clause.
fn select_into_present(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => select.into.is_some(),
        SetExpr::Query(query) => query_has_into(query),
        SetExpr::SetOperation { left, right, .. } => {
            select_into_present(left) || select_into_present(right)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_plain_select() {
        assert!(validate_select_only("SELECT 1").is_ok());
        assert!(validate_select_only(
            "select a, b FROM t WHERE a > 1 GROUP BY b ORDER BY a LIMIT 5"
        )
        .is_ok());
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

    #[test]
    fn rejects_select_into() {
        // SELECT ... INTO parses as a Query but creates a table, so it must be rejected
        // rather than relying on the downstream planner to catch it.
        assert!(validate_select_only("SELECT * INTO new_t FROM t").is_err());
        assert!(validate_select_only("SELECT a INTO new_t FROM t UNION SELECT b FROM u").is_err());
        // A CTE body is another hiding place; it must be traversed too.
        assert!(
            validate_select_only("WITH x AS (SELECT a INTO new_t FROM t) SELECT * FROM x").is_err()
        );
    }

    #[test]
    fn error_kinds_are_specific() {
        let kind = |sql: &str| validate_select_only(sql).unwrap_err().kind;
        assert_eq!(kind(""), ErrorKind::InvalidSql);
        assert_eq!(kind("SELEC 1"), ErrorKind::InvalidSql);
        assert_eq!(kind("INSERT INTO t VALUES (1)"), ErrorKind::UnsupportedSql);
        assert_eq!(kind("DROP TABLE t"), ErrorKind::UnsupportedSql);
        assert_eq!(kind("SELECT 1; SELECT 2"), ErrorKind::UnsupportedSql);
        assert_eq!(kind("SELECT * INTO x FROM t"), ErrorKind::UnsupportedSql);
    }

    #[test]
    fn allows_cte_union_and_subquery() {
        assert!(validate_select_only("WITH a AS (SELECT 1 AS n) SELECT * FROM a").is_ok());
        assert!(validate_select_only("SELECT 1 UNION SELECT 2").is_ok());
        assert!(validate_select_only("SELECT * FROM (SELECT 1 AS n) s").is_ok());
    }

    #[test]
    fn rejects_oversized_sql() {
        // A long expression chain is rejected by length before it can build a deep AST.
        let long = format!("SELECT 1 WHERE {}", "1=1 OR ".repeat(4000));
        assert!(long.len() > MAX_SQL_BYTES);
        let err = validate_select_only(&long).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidSql);
        assert!(err.message.contains("too long"), "{}", err.message);
    }
}
