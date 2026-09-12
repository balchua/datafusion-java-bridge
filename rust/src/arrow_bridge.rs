//! The Arrow C Data Interface bridge between Java and Rust.
//!
//! This module is the only place that crosses the Arrow C Data Interface in either
//! direction. See `docs/arrow-lifetime.md` for the full ownership contract.
//!
//! Inbound (Java -> Rust): Java exports schema + one `ArrowArray` struct per column
//! (zero-copy: Arrow Java only increments buffer refcounts). Rust imports the structs and
//! wraps the Java buffers. In deep-copy mode (`copy == true`) every buffer is then copied
//! into Rust-owned memory and the import is dropped, firing the release callbacks, so Java
//! may free everything once `import_batch` returns. In zero-copy mode (`copy == false`) the
//! import is kept in the returned batch, so the Java buffers stay alive until the table is
//! dropped; Java must keep them unmodified until then (see `docs/arrow-lifetime.md`).
//!
//! Outbound (Rust -> Java): Rust writes an `FFI_ArrowSchema` (once, at query start) and
//! one `FFI_ArrowArray` per column into Java-allocated struct memory. The exported
//! structs hold `Arc` references to the buffers, so they stay alive until Java closes
//! the imported vectors, which triggers the release callbacks and frees Rust buffers.

use datafusion::arrow::array::{make_array, ArrayData, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use datafusion::arrow::ffi::{
    from_ffi_and_data_type, to_ffi, FFI_ArrowArray, FFI_ArrowSchema,
};
use datafusion::arrow::datatypes::Schema;
use std::sync::Arc;

use crate::error::{EngineError, ErrorKind, Result};

fn bridge_error(message: impl Into<String>) -> EngineError {
    EngineError::new(ErrorKind::ArrowBridge, message)
}

/// Copies a buffer into a freshly allocated, `ALIGNMENT`-aligned Rust-owned buffer.
///
/// `Buffer::from(vec![])` would carry a misaligned dangling pointer (address 1), which the
/// ArrayData validator rejects for wider element types; `MutableBuffer::with_capacity`
/// produces a properly aligned (possibly empty) buffer in all cases.
fn copy_buffer(source: &Buffer) -> Buffer {
    let mut copy = MutableBuffer::with_capacity(source.len());
    copy.extend_from_slice(source.as_slice());
    copy.into()
}

/// Imports a record batch exported by Java's `Data.exportSchema` + `Data.exportVector`.
///
/// # Arguments
/// * `schema_ptr` - address of a Java-allocated `ArrowSchema` struct filled by Java
/// * `array_ptrs` - addresses of Java-allocated `ArrowArray` structs, one per column
/// * `copy` - `true`: deep-copy every buffer into Rust-owned memory (ownership decoupled
///   from the JVM). `false`: zero-copy — the returned batch wraps the Java buffers via the
///   C Data Interface owner, and they stay alive until the batch is dropped (which fires
///   Java's release callback). Zero-copy requires the Java buffers to remain unmodified
///   for the batch's lifetime; `align_buffers()` may still copy individual misaligned
///   buffers.
///
/// The imported C structs are consumed (per the C data interface "move" semantics) and
/// released before this function returns for the schema (which is always materialized
/// into a Rust `Schema`). Array buffers are released when the returned batch is dropped.
pub fn import_batch(
    schema_ptr: usize,
    array_ptrs: &[usize],
    copy: bool,
) -> Result<(Schema, RecordBatch)> {
    // SAFETY: `schema_ptr` points at a fully initialized `ArrowSchema` written by Arrow
    // Java's exporter. `from_raw` atomically replaces the struct contents with an empty
    // struct (release = NULL), so Java-side code can safely close/free its own struct
    // memory afterwards without double-release. We own the original contents from here on;
    // dropping the `FFI_ArrowSchema` invokes Java's release callback.
    let ffi_schema =
        unsafe { FFI_ArrowSchema::from_raw(schema_ptr as *mut FFI_ArrowSchema) };
    let schema = Schema::try_from(&ffi_schema)
        .map_err(|e| bridge_error(format!("schema import failed: {e}")))?;
    drop(ffi_schema); // release callback -> Java frees schema private data

    if array_ptrs.len() != schema.fields().len() {
        return Err(bridge_error(format!(
            "column count mismatch: schema has {} fields, got {} arrays",
            schema.fields().len(),
            array_ptrs.len()
        )));
    }

    let mut columns = Vec::with_capacity(array_ptrs.len());
    for (i, ptr) in array_ptrs.iter().enumerate() {
        // SAFETY: same contract as `ffi_schema` above; `from_raw` consumes the struct
        // contents and we are responsible for dropping it (which releases Java refs).
        let ffi_array = unsafe { FFI_ArrowArray::from_raw(*ptr as *mut FFI_ArrowArray) };
        let imported = unsafe {
            from_ffi_and_data_type(ffi_array, schema.field(i).data_type().clone())
        }
        .map_err(|e| bridge_error(format!("array import failed for column {i}: {e}")))?;

        let data = if copy {
            // Deep-copy mode: Java's buffers are copied into Rust-owned memory so
            // DataFusion's lifetime is fully decoupled from the JVM.
            deep_copy(&imported)?
        } else {
            // Zero-copy mode: keep the imported ArrayData, whose buffers wrap the Java
            // memory and hold the C Data Interface owner. Dropping the batch triggers the
            // release callback, which frees the Java-side buffers.
            imported
        };
        columns.push(make_array(data));
    }

    let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| bridge_error(format!("record batch import failed: {e}")))?;
    Ok((schema, batch))
}

/// Deep-copies `data` (including nested children and dictionary values) into
/// freshly allocated Rust-owned buffers. Buffer contents are copied with `memcpy`;
/// structure (offsets, lengths, null bitmaps) is preserved exactly.
fn deep_copy(data: &ArrayData) -> Result<ArrayData> {
    let buffers: Vec<Buffer> = data.buffers().iter().map(copy_buffer).collect();

    let child_data = data
        .child_data()
        .iter()
        .map(deep_copy)
        .collect::<Result<Vec<ArrayData>>>()?;

    let nulls = data.nulls().map(|nb| {
        let bb = nb.inner();
        // Copy the raw bits; offset/len are preserved so validity semantics are identical.
        let boolean = BooleanBuffer::new(copy_buffer(bb.inner()), bb.offset(), bb.len());
        NullBuffer::new(boolean)
    });

    // SAFETY: none required; `into_builder` takes ownership of a clone of the imported
    // data and we replace every buffer with owned memory of identical length. The builder
    // keeps len/offset/data_type, so the rebuilt ArrayData is structurally identical to
    // `data`, only backed by Rust-owned memory.
    data.clone()
        .into_builder()
        .buffers(buffers)
        .child_data(child_data)
        .nulls(nulls)
        .build()
        .map_err(|e| bridge_error(format!("deep copy failed: {e}")))
}

/// Exports a Rust schema into a Java-allocated `ArrowSchema` struct.
///
/// The exported schema is struct-typed (children = record fields), which is what Arrow
/// Java's `Data.importSchema` expects for a record batch schema.
pub fn export_schema(schema: &Schema, out_ptr: usize) -> Result<()> {
    let ffi = FFI_ArrowSchema::try_from(schema)
        .map_err(|e| bridge_error(format!("schema export failed: {e}")))?;
    // SAFETY: `out_ptr` points at Java-allocated `ArrowSchema` struct memory. `ptr::write`
    // moves the exported struct into that memory (bitwise copy, no destructor runs on the
    // source); ownership of the release callback transfers to Java. Rust must not touch
    // this memory again.
    unsafe { std::ptr::write(out_ptr as *mut FFI_ArrowSchema, ffi) };
    Ok(())
}

/// Exports one column of a record batch into a Java-allocated `ArrowArray` struct.
///
/// The exported struct holds `Arc` clones of the column's buffers, so the data outlives
/// the batch this column came from; Java closing the imported vector triggers the release
/// callback that frees them.
pub fn export_column(data: &ArrayData, out_ptr: usize) -> Result<()> {
    let (array, _schema) = to_ffi(data)
        .map_err(|e| bridge_error(format!("column export failed: {e}")))?;
    // The schema half is not needed (Java caches the schema from the first import);
    // dropping it releases the schema's private data.
    drop(_schema);
    // SAFETY: `out_ptr` points at Java-allocated `ArrowArray` struct memory. Same move
    // semantics as `export_schema`.
    unsafe { std::ptr::write(out_ptr as *mut FFI_ArrowArray, array) };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int32Array, Int64Array,
        ListArray, StringArray, StructArray, TimestampMillisecondArray,
    };
    use datafusion::arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    /// Round-trip a batch through our own export+import using locally allocated structs.
    /// This exercises exactly the same code path the JNI layer uses; the Java side is
    /// exercised in Java integration tests.
    fn round_trip(batch: &RecordBatch) -> RecordBatch {
        let schema = batch.schema();
        // Export into locally allocated struct memory (what Java would provide).
        // `usize` arrays guarantee the 8-byte alignment the structs require.
        let mut schema_mem = [0usize; 24];
        let mut array_mems: Vec<[usize; 24]> =
            (0..batch.num_columns()).map(|_| [0usize; 24]).collect();
        export_schema(&schema, schema_mem.as_mut_ptr() as usize).unwrap();
        for (i, mem) in array_mems.iter_mut().enumerate() {
            export_column(&batch.column(i).to_data(), mem.as_mut_ptr() as usize).unwrap();
        }

        import_batch(
            schema_mem.as_ptr() as usize,
            &array_mems.iter().map(|b| b.as_ptr() as usize).collect::<Vec<_>>(),
            true,
        )
        .unwrap()
        .1
    }

    fn assert_batches_equal(left: &RecordBatch, right: &RecordBatch) {
        assert_eq!(left.num_rows(), right.num_rows());
        assert_eq!(left.schema(), right.schema());
        for i in 0..left.num_columns() {
            assert_eq!(
                format!("{:?}", left.column(i)),
                format!("{:?}", right.column(i)),
                "column {i} differs"
            );
        }
    }

    #[test]
    fn round_trip_all_types() {
        let account_field = Field::new_struct(
            "account",
            vec![
                Arc::new(Field::new("iban", DataType::Utf8, true)),
                Arc::new(Field::new("number", DataType::Utf8, true)),
            ],
            true,
        );
        let list_field = Field::new(
            "structured_remittance",
            DataType::List(Arc::new(Field::new_struct(
                "item",
                vec![
                    Arc::new(Field::new("code", DataType::Utf8, true)),
                    Arc::new(Field::new("reference", DataType::Utf8, true)),
                    Arc::new(Field::new("amount", DataType::Decimal128(18, 4), true)),
                ],
                true,
            ))),
            true,
        );
        let schema = Schema::new(vec![
            Arc::new(Field::new("transaction_id", DataType::Utf8, false)),
            Arc::new(Field::new("amount", DataType::Decimal128(18, 4), true)),
            Arc::new(Field::new("currency", DataType::Utf8, true)),
            Arc::new(Field::new("quantity", DataType::Int32, true)),
            Arc::new(Field::new("big", DataType::Int64, true)),
            Arc::new(Field::new("flag", DataType::Boolean, true)),
            Arc::new(Field::new("created", DataType::Timestamp(TimeUnit::Millisecond, None), true)),
            Arc::new(Field::new("exec_date", DataType::Date32, true)),
            Arc::new(Field::new_struct(
                "debtor",
                vec![
                    Arc::new(Field::new("name", DataType::Utf8, true)),
                    Arc::new(account_field.clone()),
                ],
                true,
            )),
            Arc::new(list_field),
        ]);

        let debtor_account = StructArray::from(vec![
            (
                Arc::new(Field::new("iban", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("DE89370400440532013000"), None])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("number", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("123456"), Some("99")])) as ArrayRef,
            ),
        ]);
        let debtor = StructArray::from(vec![
            (
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("AAA"), Some("BBB")])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("account", account_field.data_type().clone(), true)),
                Arc::new(debtor_account) as ArrayRef,
            ),
        ]);

        // List<Struct> with 1-3 elements per row, with a null row.
        let items = StructArray::from(vec![
            (
                Arc::new(Field::new("code", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("SCOR"), Some("SCOR"), Some("SCOR")])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("reference", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some("REF-1"), Some("REF-2"), Some("REF-3")])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("amount", DataType::Decimal128(18, 4), true)),
                Arc::new(
                    Decimal128Array::from(vec![Some(1234), Some(5678), Some(-90)])
                        .with_precision_and_scale(18, 4)
                        .unwrap(),
                ) as ArrayRef,
            ),
        ]);
        let list = ListArray::new(
            Arc::new(Field::new("item", items.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 3])),
            Arc::new(items),
            Some(arrow::buffer::NullBuffer::from(vec![true, false])),
        );

        let batch = RecordBatch::try_new(Arc::new(schema), vec![
            Arc::new(StringArray::from(vec!["TXN-1", "TXN-2"])),
            Arc::new(
                Decimal128Array::from(vec![Some(1000050), None])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
            ),
            Arc::new(StringArray::from(vec![Some("EUR"), None])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
            Arc::new(Int64Array::from(vec![Some(9876543210), Some(-5)])),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1772995200000), None])),
            Arc::new(Date32Array::from(vec![Some(20000), None])),
            Arc::new(debtor),
            Arc::new(list),
        ])
        .unwrap();

        let out = round_trip(&batch);
        assert_batches_equal(&batch, &out);
    }

    #[test]
    fn round_trip_empty_batch() {
        let schema = Schema::new(vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ]);
        let batch = RecordBatch::new_empty(Arc::new(schema));
        let out = round_trip(&batch);
        assert_batches_equal(&batch, &out);
    }
}