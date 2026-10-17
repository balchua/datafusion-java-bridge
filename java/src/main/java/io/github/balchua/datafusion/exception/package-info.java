/**
 * Typed exceptions raised by the native DataFusion engine.
 *
 * <p>The native layer maps a Rust {@code EngineError.kind} one-to-one to one of these
 * classes (see {@code rust/src/error.rs}), so callers can branch on the exception type:
 *
 * <ul>
 *   <li>{@link io.github.balchua.datafusion.exception.SqlParseException} — invalid SQL syntax</li>
 *   <li>{@link io.github.balchua.datafusion.exception.SqlUnsupportedException} — parseable but not a read-only SELECT</li>
 *   <li>{@link io.github.balchua.datafusion.exception.TableNotFoundException} — a referenced table does not exist</li>
 *   <li>{@link io.github.balchua.datafusion.exception.SchemaMismatchException} — Arrow/Parquet schema mismatch or bad table name</li>
 *   <li>{@link io.github.balchua.datafusion.exception.ArrowBridgeException} — Arrow C Data Interface import/export failure</li>
 *   <li>{@link io.github.balchua.datafusion.exception.ParquetException} — Parquet read/write failure</li>
 *   <li>{@link io.github.balchua.datafusion.exception.ObjectStoreException} — object-store configuration or operation failure</li>
 *   <li>{@link io.github.balchua.datafusion.exception.ResourceException} — a configured resource limit was hit</li>
 *   <li>{@link io.github.balchua.datafusion.exception.QueryCancelledException} — the query was cancelled</li>
 *   <li>{@link io.github.balchua.datafusion.exception.EngineClosedException} — the engine or handle is closed</li>
 *   <li>{@link io.github.balchua.datafusion.exception.QueryNotFoundException} — the query id is not registered</li>
 *   <li>{@link io.github.balchua.datafusion.exception.DataFusionException} — base class / any other native error</li>
 * </ul>
 */
package io.github.balchua.datafusion.exception;
