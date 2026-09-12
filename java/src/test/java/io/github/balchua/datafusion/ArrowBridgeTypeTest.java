package io.github.balchua.datafusion;

import io.github.balchua.datafusion.engine.DataFusionEngine;
import io.github.balchua.datafusion.engine.EngineConfig;
import io.github.balchua.datafusion.engine.QueryResult;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Bridge correctness for every Arrow type incl. nulls (spec §29). Data is built in Java,
 * registered in the engine, queried back with projections, and compared to the Java side.
 */
class ArrowBridgeTypeTest {

    private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        allocator.close();
    }

    private static Schema mixedSchema() {
        return new Schema(List.of(
                new Field("s", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("i32", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("i64", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("flag", FieldType.nullable(new ArrowType.Bool()), null),
                new Field("dec", FieldType.nullable(new ArrowType.Decimal(18, 4, 128)), null),
                new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null),
                new Field("day", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
                new Field("st", FieldType.nullable(new ArrowType.Struct()), List.of(
                        new Field("inner_i", FieldType.nullable(new ArrowType.Int(32, true)), null),
                        new Field("inner_s", FieldType.nullable(new ArrowType.Utf8()), null)
                )),
                new Field("lst", FieldType.nullable(new ArrowType.List()),
                        List.of(new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null)))
        ));
    }

    @org.junit.jupiter.api.Test
    void allTypesWithNullsRoundTripThroughEngine() {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(mixedSchema(), allocator)) {
            VarCharVector s = (VarCharVector) root.getVector("s");
            IntVector i32 = (IntVector) root.getVector("i32");
            BigIntVector i64 = (BigIntVector) root.getVector("i64");
            BitVector flag = (BitVector) root.getVector("flag");
            DecimalVector dec = (DecimalVector) root.getVector("dec");
            TimeStampMilliVector ts = (TimeStampMilliVector) root.getVector("ts");
            DateDayVector day = (DateDayVector) root.getVector("day");
            StructVector st = (StructVector) root.getVector("st");
            IntVector innerI = (IntVector) st.getChild("inner_i");
            VarCharVector innerS = (VarCharVector) st.getChild("inner_s");
            ListVector lst = (ListVector) root.getVector("lst");
            IntVector lstData = (IntVector) lst.getDataVector();

            // row 0: fully populated; struct + list(2 items)
            s.setSafe(0, "hello".getBytes());
            i32.setSafe(0, 1);
            i64.setSafe(0, 1234567890123L);
            flag.setSafe(0, 1);
            dec.setSafe(0, BigDecimal.valueOf(1234.5).setScale(4, RoundingMode.UNNECESSARY));
            
            TimeStampMilliHolder tsHolder = new TimeStampMilliHolder();
            tsHolder.value = 1772995200000L;
            ts.setSafe(0, tsHolder);
            day.setSafe(0, 20000);
            st.setIndexDefined(0);
            innerI.setSafe(0, 7);
            innerS.setSafe(0, "A".getBytes());
            lst.startNewValue(0);
            lstData.setSafe(0, 10);
            lstData.setSafe(1, 20);
            lst.endValue(0, 2);

            // row 1: nulls in s, dec; list with a null element
            s.setNull(1);
            i32.setSafe(1, 2);
            i64.setSafe(1, -5L);
            flag.setSafe(1, 0);
            dec.setNull(1);
            
            tsHolder.value = 1772995201000L;
            ts.setSafe(1, tsHolder);
            day.setSafe(1, 20001);
            st.setNull(1);
            lst.startNewValue(1);
            lstData.setNull(2);
            lst.endValue(1, 1);

            // row 2: null list, populated struct
            s.setSafe(2, "tail".getBytes());
            i32.setNull(2);
            i64.setSafe(2, 42L);
            flag.setNull(2);
            dec.setSafe(2, BigDecimal.valueOf(-0.5).setScale(4, RoundingMode.UNNECESSARY));
            
            tsHolder.value = 1772995202000L;
            ts.setSafe(2, tsHolder);
            day.setNull(2);
            st.setIndexDefined(2);
            innerI.setSafe(2, 9);
            innerS.setSafe(2, "C".getBytes());
            lst.setNull(2);

            root.setRowCount(3);

            try (DataFusionEngine engine = new DataFusionEngine(EngineConfig.defaults())) {
                engine.registerArrow("mixed", root);

                // ORDER BY s NULLS LAST => rows: hello, tail, (s = null)
                try (QueryResult result = engine.query(
                        "SELECT s, i32, i64, flag, dec, ts, day, st, lst FROM mixed ORDER BY s NULLS LAST")) {
                    VectorSchemaRoot out = result.next();
                    try (out) {
                        assertEquals(3, out.getRowCount());
                        assertScalarColumns(out);
                        assertNestedColumns(out);
                    }
                }
            }
        }
    }

    private static void assertScalarColumns(VectorSchemaRoot out) {
        VarCharVector os = (VarCharVector) out.getVector("s");
        assertEquals("hello", os.getObject(0).toString());
        assertEquals("tail", os.getObject(1).toString());
        assertNull(os.getObject(2));

        IntVector oi32 = (IntVector) out.getVector("i32");
        assertEquals(1, (int) oi32.getObject(0));
        assertNull(oi32.getObject(1)); // tail row: i32 was null
        assertEquals(2, (int) oi32.getObject(2));

        BigIntVector oi64 = (BigIntVector) out.getVector("i64");
        assertEquals(1234567890123L, oi64.getObject(0));
        assertEquals(42L, oi64.getObject(1));
        assertEquals(-5L, oi64.getObject(2));

        BitVector oflag = (BitVector) out.getVector("flag");
        assertEquals(Boolean.TRUE, oflag.getObject(0));
        assertNull(oflag.getObject(1));
        assertEquals(Boolean.FALSE, oflag.getObject(2));

        DecimalVector odec = (DecimalVector) out.getVector("dec");
        assertEquals(BigDecimal.valueOf(1234.5).setScale(4, RoundingMode.UNNECESSARY),
                odec.getObject(0));
        assertEquals(BigDecimal.valueOf(-0.5).setScale(4, RoundingMode.UNNECESSARY),
                odec.getObject(1));
        assertNull(odec.getObject(2));

        TimeStampMilliVector ots = (TimeStampMilliVector) out.getVector("ts");
        assertEquals(1772995200000L, ots.get(0));
        assertEquals(1772995202000L, ots.get(1));
        assertEquals(1772995201000L, ots.get(2));

        DateDayVector oday = (DateDayVector) out.getVector("day");
        assertEquals(20000, (int) oday.getObject(0));
        assertNull(oday.getObject(1));
        assertEquals(20001, (int) oday.getObject(2));
    }

    private static void assertNestedColumns(VectorSchemaRoot out) {
        StructVector ost = (StructVector) out.getVector("st");
        IntVector oInnerI = (IntVector) ost.getChild("inner_i");
        VarCharVector oInnerS = (VarCharVector) ost.getChild("inner_s");
        assertEquals(7, (int) oInnerI.getObject(0));
        assertEquals("A", oInnerS.getObject(0).toString());
        assertEquals(9, (int) oInnerI.getObject(1));
        assertNull(ost.getObject(2)); // whole struct null

        ListVector olst = (ListVector) out.getVector("lst");
        assertEquals(List.of(10, 20), olst.getObject(0));
        assertNull(olst.getObject(1)); // tail row: list itself is null
        // row 2 (s = null): list [null]
        @SuppressWarnings("unchecked")
        List<Integer> nullElementList = (List<Integer>) olst.getObject(2);
        assertEquals(1, nullElementList.size());
        assertNull(nullElementList.get(0));
    }
}