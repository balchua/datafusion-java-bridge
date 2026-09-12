package io.github.balchua.datafusion.bench.pain;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/** Helpers to fill the flat pain.001 Arrow vectors. */
final class PainVectors {

    private PainVectors() {}

    /**
     * Pre-allocates each transaction vector for {@code rows} values so a single root does not
     * repeatedly reallocate while growing. For variable-width columns the density is the
     * estimated average bytes per value.
     */
    static void presetCapacity(VectorSchemaRoot root, int rows) {
        for (FieldVector vector : root.getFieldVectors()) {
            if (vector instanceof BaseVariableWidthVector variable) {
                variable.setInitialCapacity(rows, 32.0);
            } else if (vector instanceof BaseFixedWidthVector fixed) {
                fixed.setInitialCapacity(rows);
            }
        }
    }

    static void setVarChar(VectorSchemaRoot root, String fieldName, int index, String value) {
        VarCharVector vec = (VarCharVector) root.getVector(fieldName);
        if (value != null) {
            vec.setSafe(index, value.getBytes(StandardCharsets.UTF_8));
        } else {
            vec.setNull(index);
        }
    }

    static void setDecimal(VectorSchemaRoot root, String fieldName, int index, BigDecimal value) {
        DecimalVector vec = (DecimalVector) root.getVector(fieldName);
        if (value != null) {
            vec.setSafe(index, value.setScale(vec.getScale(), RoundingMode.HALF_UP));
        } else {
            vec.setNull(index);
        }
    }

    static void setDateDay(VectorSchemaRoot root, String fieldName, int index, String isoDateStr) {
        DateDayVector vec = (DateDayVector) root.getVector(fieldName);
        if (isoDateStr != null) {
            LocalDate date = LocalDate.parse(isoDateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            vec.setSafe(index, (int) date.toEpochDay());
        } else {
            vec.setNull(index);
        }
    }
}
