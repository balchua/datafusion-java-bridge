package io.github.balchua.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A deliberately non-payment sample dataset (students / classrooms / enrollments) used by
 * the concurrency and error-path tests. It proves the bridge is data-model agnostic while
 * still stressing joins, decimals, strings, nullability and a boolean column.
 *
 * <p>Rows are tagged with a caller-supplied prefix so several engines can hold otherwise
 * identical table names with distinct contents; a cross-engine mix-up then changes counts.
 */
public final class SchoolData {
    public static final String TABLE_STUDENTS = "students";
    public static final String TABLE_CLASSROOMS = "classrooms";
    public static final String TABLE_ENROLLMENTS = "enrollments";

    public static final String[] SUBJECTS = {"MATH", "SCIENCE", "LITERATURE", "ART"};

    private SchoolData() {}

    /** The three tables, closed together. */
    public record Dataset(VectorSchemaRoot students,
                          VectorSchemaRoot classrooms,
                          VectorSchemaRoot enrollments) implements AutoCloseable {
        @Override
        public void close() {
            students.close();
            classrooms.close();
            enrollments.close();
        }
    }

    public static Dataset build(BufferAllocator allocator, String tag,
                                int studentCount, int enrollmentCount) {
        return new Dataset(
                students(allocator, tag, studentCount),
                classrooms(allocator, tag),
                enrollments(allocator, tag, studentCount, enrollmentCount));
    }

    private static VectorSchemaRoot students(BufferAllocator allocator, String tag, int count) {
        VectorSchemaRoot root = VectorSchemaRoot.create(studentsSchema(), allocator);
        VarCharVector id = (VarCharVector) root.getVector("student_id");
        VarCharVector name = (VarCharVector) root.getVector("name");
        DecimalVector gpa = (DecimalVector) root.getVector("gpa");
        BitVector enrolled = (BitVector) root.getVector("enrolled");
        for (int k = 0; k < count; k++) {
            id.setSafe(k, utf8(tag + "-S" + k));
            name.setSafe(k, utf8(tag + "-name" + k));
            gpa.setSafe(k, BigDecimal.valueOf((k * 7) % 400, 2));
            enrolled.setSafe(k, k % 2 == 0 ? 1 : 0);
        }
        root.setRowCount(count);
        return root;
    }

    private static VectorSchemaRoot classrooms(BufferAllocator allocator, String tag) {
        VectorSchemaRoot root = VectorSchemaRoot.create(classroomsSchema(), allocator);
        VarCharVector id = (VarCharVector) root.getVector("classroom_id");
        VarCharVector subject = (VarCharVector) root.getVector("subject");
        IntVector capacity = (IntVector) root.getVector("capacity");
        for (int k = 0; k < SUBJECTS.length; k++) {
            id.setSafe(k, utf8(tag + "-C" + k));
            subject.setSafe(k, utf8(SUBJECTS[k]));
            capacity.setSafe(k, 20 + k * 5);
        }
        root.setRowCount(SUBJECTS.length);
        return root;
    }

    private static VectorSchemaRoot enrollments(BufferAllocator allocator, String tag,
                                                int studentCount, int count) {
        VectorSchemaRoot root = VectorSchemaRoot.create(enrollmentsSchema(), allocator);
        VarCharVector id = (VarCharVector) root.getVector("enrollment_id");
        VarCharVector student = (VarCharVector) root.getVector("student_id");
        VarCharVector classroom = (VarCharVector) root.getVector("classroom_id");
        DecimalVector score = (DecimalVector) root.getVector("score");
        for (int k = 0; k < count; k++) {
            id.setSafe(k, utf8(tag + "-E" + k));
            student.setSafe(k, utf8(tag + "-S" + (k % studentCount)));
            classroom.setSafe(k, utf8(tag + "-C" + (k % SUBJECTS.length)));
            if (k % 17 == 0) {
                score.setNull(k); // nullable score
            } else {
                score.setSafe(k, BigDecimal.valueOf((k * 13L) % 10000, 2));
            }
        }
        root.setRowCount(count);
        return root;
    }

    // ---- expected values (mirror the generation rules) --------------------

    /** Number of enrollments for one student (studentIndex), before null filtering. */
    public static long studentEnrollmentCount(int studentCount, int enrollmentCount, int studentIndex) {
        long n = 0;
        for (int k = 0; k < enrollmentCount; k++) {
            if (k % studentCount == studentIndex) {
                n++;
            }
        }
        return n;
    }

    /** SUM(score) over one student's enrollments, skipping null scores. */
    public static BigDecimal studentScoreSum(int studentCount, int enrollmentCount, int studentIndex) {
        BigDecimal sum = BigDecimal.ZERO;
        for (int k = 0; k < enrollmentCount; k++) {
            if (k % studentCount == studentIndex && k % 17 != 0) {
                sum = sum.add(BigDecimal.valueOf((k * 13L) % 10000, 2));
            }
        }
        return sum;
    }

    /** Number of enrollments whose classroom is MATH (classroom index 0). */
    public static long mathEnrollmentCount(int enrollmentCount) {
        long n = 0;
        for (int k = 0; k < enrollmentCount; k++) {
            if (k % SUBJECTS.length == 0) {
                n++;
            }
        }
        return n;
    }

    // ---- schemas ----------------------------------------------------------

    private static Schema studentsSchema() {
        return new Schema(List.of(
                utf8Field("student_id", false),
                utf8Field("name", true),
                decimalField("gpa", 4, 2, true),
                boolField("enrolled", true)));
    }

    private static Schema classroomsSchema() {
        return new Schema(List.of(
                utf8Field("classroom_id", false),
                utf8Field("subject", true),
                intField("capacity", false)));
    }

    private static Schema enrollmentsSchema() {
        return new Schema(List.of(
                utf8Field("enrollment_id", false),
                utf8Field("student_id", false),
                utf8Field("classroom_id", false),
                decimalField("score", 5, 2, true)));
    }

    private static Field utf8Field(String name, boolean nullable) {
        return new Field(name, fieldType(nullable, new ArrowType.Utf8()), null);
    }

    private static Field intField(String name, boolean nullable) {
        return new Field(name, fieldType(nullable, new ArrowType.Int(32, true)), null);
    }

    private static Field decimalField(String name, int precision, int scale, boolean nullable) {
        return new Field(name, fieldType(nullable, new ArrowType.Decimal(precision, scale, 128)), null);
    }

    private static Field boolField(String name, boolean nullable) {
        return new Field(name, fieldType(nullable, new ArrowType.Bool()), null);
    }

    private static FieldType fieldType(boolean nullable, ArrowType type) {
        return nullable ? FieldType.nullable(type) : FieldType.notNullable(type);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
