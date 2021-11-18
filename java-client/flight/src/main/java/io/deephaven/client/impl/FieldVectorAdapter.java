package io.deephaven.client.impl;

import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.BooleanArray;
import io.deephaven.qst.array.ByteArray;
import io.deephaven.qst.array.CharArray;
import io.deephaven.qst.array.DoubleArray;
import io.deephaven.qst.array.FloatArray;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.IntArray;
import io.deephaven.qst.array.LongArray;
import io.deephaven.qst.array.PrimitiveArray;
import io.deephaven.qst.array.ShortArray;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.time.Instant;
import java.util.Objects;

/**
 * Utilities for creating {@link FieldVector}.
 */
public class FieldVectorAdapter implements Array.Visitor, PrimitiveArray.Visitor {

    /**
     * Convert a {@code column} into a {@link FieldVector}.
     *
     * @param column the column
     * @param allocator the allocator
     * @return the field vector
     */
    public static FieldVector of(Column<?> column, BufferAllocator allocator) {
        return of(column.name(), column.array(), allocator);
    }

    /**
     * Convert a {@code name} and an {@code array} into a {@link FieldVector}.
     *
     * @param name the column name
     * @param array the array
     * @param allocator the allocator
     * @return the field vector
     */
    public static FieldVector of(String name, Array<?> array, BufferAllocator allocator) {
        return array.walk(new FieldVectorAdapter(name, allocator)).out();
    }

    private final String name;
    private final BufferAllocator allocator;

    private FieldVector out;

    private FieldVectorAdapter(String name, BufferAllocator allocator) {
        this.name = Objects.requireNonNull(name);
        this.allocator = Objects.requireNonNull(allocator);
    }

    FieldVector out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(PrimitiveArray<?> primitive) {
        primitive.walk((PrimitiveArray.Visitor) this);
    }

    @Override
    public void visit(GenericArray<?> generic) {
        generic.componentType().walk(new Visitor() {
            @Override
            public void visit(StringType stringType) {
                visitStringArray(generic.cast(stringType));
            }

            @Override
            public void visit(InstantType instantType) {
                visitInstantArray(generic.cast(instantType));
            }

            @Override
            public void visit(ArrayType<?, ?> arrayType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visit(CustomType<?> customType) {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Override
    public void visit(ByteArray byteArray) {
        Field field = FieldAdapter.byteField(name);
        TinyIntVector vector = new TinyIntVector(field, allocator);
        VectorHelper.fill(vector, byteArray.values(), 0, byteArray.size());
        out = vector;
    }

    @Override
    public void visit(BooleanArray booleanArray) {
        // TODO(deephaven-core#43): Do not reinterpret bool as byte
        Field field = FieldAdapter.booleanField(name);
        TinyIntVector vector = new TinyIntVector(field, allocator);
        VectorHelper.fill(vector, booleanArray.values(), 0, booleanArray.size());
        out = vector;
    }

    @Override
    public void visit(CharArray charArray) {
        Field field = FieldAdapter.charField(name);
        UInt2Vector vector = new UInt2Vector(field, allocator);
        VectorHelper.fill(vector, charArray.values(), 0, charArray.size());
        out = vector;
    }

    @Override
    public void visit(ShortArray shortArray) {
        Field field = FieldAdapter.shortField(name);
        SmallIntVector vector = new SmallIntVector(field, allocator);
        VectorHelper.fill(vector, shortArray.values(), 0, shortArray.size());
        out = vector;
    }

    @Override
    public void visit(IntArray intArray) {
        Field field = FieldAdapter.intField(name);
        IntVector vector = new IntVector(field, allocator);
        VectorHelper.fill(vector, intArray.values(), 0, intArray.size());
        out = vector;
    }

    @Override
    public void visit(LongArray longArray) {
        Field field = FieldAdapter.longField(name);
        BigIntVector vector = new BigIntVector(field, allocator);
        VectorHelper.fill(vector, longArray.values(), 0, longArray.size());
        out = vector;
    }

    @Override
    public void visit(FloatArray floatArray) {
        Field field = FieldAdapter.floatField(name);
        Float4Vector vector = new Float4Vector(field, allocator);
        VectorHelper.fill(vector, floatArray.values(), 0, floatArray.size());
        out = vector;
    }

    @Override
    public void visit(DoubleArray doubleArray) {
        Field field = FieldAdapter.doubleField(name);
        Float8Vector vector = new Float8Vector(field, allocator);
        VectorHelper.fill(vector, doubleArray.values(), 0, doubleArray.size());
        out = vector;
    }

    void visitStringArray(GenericArray<String> stringArray) {
        Field field = FieldAdapter.stringField(name);
        VarCharVector vector = new VarCharVector(field, allocator);
        VectorHelper.fill(vector, stringArray.values());
        out = vector;
    }

    void visitInstantArray(GenericArray<Instant> instantArray) {
        Field field = FieldAdapter.instantField(name);
        TimeStampNanoTZVector vector = new TimeStampNanoTZVector(field, allocator);
        VectorHelper.fill(vector, instantArray.values());
        out = vector;
    }
}
