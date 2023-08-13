/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
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
public class FieldVectorAdapter implements Array.Visitor<FieldVector>, PrimitiveArray.Visitor<FieldVector> {

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
        return array.walk(new FieldVectorAdapter(name, allocator));
    }

    private final String name;
    private final BufferAllocator allocator;

    private FieldVectorAdapter(String name, BufferAllocator allocator) {
        this.name = Objects.requireNonNull(name);
        this.allocator = Objects.requireNonNull(allocator);
    }

    @Override
    public FieldVector visit(PrimitiveArray<?> primitive) {
        return primitive.walk((PrimitiveArray.Visitor<FieldVector>) this);
    }

    @Override
    public FieldVector visit(GenericArray<?> generic) {
        return generic.componentType().walk(new Visitor<FieldVector>() {
            @Override
            public FieldVector visit(BoxedType<?> boxedType) {
                return boxedType.walk(new BoxedType.Visitor<FieldVector>() {
                    @Override
                    public FieldVector visit(BoxedBooleanType booleanType) {
                        return visitBooleanArray(generic.cast(booleanType));
                    }

                    @Override
                    public FieldVector visit(BoxedByteType byteType) {
                        return visitByteArray(generic.cast(byteType));
                    }

                    @Override
                    public FieldVector visit(BoxedCharType charType) {
                        return visitCharacterArray(generic.cast(charType));
                    }

                    @Override
                    public FieldVector visit(BoxedShortType shortType) {
                        return visitShortArray(generic.cast(shortType));
                    }

                    @Override
                    public FieldVector visit(BoxedIntType intType) {
                        return visitIntegerArray(generic.cast(intType));
                    }

                    @Override
                    public FieldVector visit(BoxedLongType longType) {
                        return visitLongArray(generic.cast(longType));
                    }

                    @Override
                    public FieldVector visit(BoxedFloatType floatType) {
                        return visitFloatArray(generic.cast(floatType));
                    }

                    @Override
                    public FieldVector visit(BoxedDoubleType doubleType) {
                        return visitDoubleArray(generic.cast(doubleType));
                    }
                });
            }

            @Override
            public FieldVector visit(StringType stringType) {
                return visitStringArray(generic.cast(stringType));
            }

            @Override
            public FieldVector visit(InstantType instantType) {
                return visitInstantArray(generic.cast(instantType));
            }

            @Override
            public FieldVector visit(ArrayType<?, ?> arrayType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public FieldVector visit(CustomType<?> customType) {
                throw new UnsupportedOperationException();
            }
        });
    }

    @Override
    public FieldVector visit(ByteArray byteArray) {
        Field field = FieldAdapter.byteField(name);
        TinyIntVector vector = new TinyIntVector(field, allocator);
        VectorHelper.fill(vector, byteArray.values(), 0, byteArray.size());
        return vector;
    }

    @Override
    public FieldVector visit(BooleanArray booleanArray) {
        Field field = FieldAdapter.booleanField(name);
        BitVector vector = new BitVector(field, allocator);
        VectorHelper.fill(vector, booleanArray, 0, booleanArray.size());
        return vector;
    }

    @Override
    public FieldVector visit(CharArray charArray) {
        Field field = FieldAdapter.charField(name);
        UInt2Vector vector = new UInt2Vector(field, allocator);
        VectorHelper.fill(vector, charArray.values(), 0, charArray.size());
        return vector;
    }

    @Override
    public FieldVector visit(ShortArray shortArray) {
        Field field = FieldAdapter.shortField(name);
        SmallIntVector vector = new SmallIntVector(field, allocator);
        VectorHelper.fill(vector, shortArray.values(), 0, shortArray.size());
        return vector;
    }

    @Override
    public FieldVector visit(IntArray intArray) {
        Field field = FieldAdapter.intField(name);
        IntVector vector = new IntVector(field, allocator);
        VectorHelper.fill(vector, intArray.values(), 0, intArray.size());
        return vector;
    }

    @Override
    public FieldVector visit(LongArray longArray) {
        Field field = FieldAdapter.longField(name);
        BigIntVector vector = new BigIntVector(field, allocator);
        VectorHelper.fill(vector, longArray.values(), 0, longArray.size());
        return vector;
    }

    @Override
    public FieldVector visit(FloatArray floatArray) {
        Field field = FieldAdapter.floatField(name);
        Float4Vector vector = new Float4Vector(field, allocator);
        VectorHelper.fill(vector, floatArray.values(), 0, floatArray.size());
        return vector;
    }

    @Override
    public FieldVector visit(DoubleArray doubleArray) {
        Field field = FieldAdapter.doubleField(name);
        Float8Vector vector = new Float8Vector(field, allocator);
        VectorHelper.fill(vector, doubleArray.values(), 0, doubleArray.size());
        return vector;
    }

    FieldVector visitBooleanArray(GenericArray<Boolean> array) {
        Field field = FieldAdapter.booleanField(name);
        BitVector vector = new BitVector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitByteArray(GenericArray<Byte> array) {
        Field field = FieldAdapter.byteField(name);
        TinyIntVector vector = new TinyIntVector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitCharacterArray(GenericArray<Character> array) {
        Field field = FieldAdapter.charField(name);
        UInt2Vector vector = new UInt2Vector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitShortArray(GenericArray<Short> array) {
        Field field = FieldAdapter.shortField(name);
        SmallIntVector vector = new SmallIntVector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitIntegerArray(GenericArray<Integer> array) {
        Field field = FieldAdapter.intField(name);
        IntVector vector = new IntVector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitLongArray(GenericArray<Long> array) {
        Field field = FieldAdapter.longField(name);
        BigIntVector vector = new BigIntVector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitFloatArray(GenericArray<Float> array) {
        Field field = FieldAdapter.floatField(name);
        Float4Vector vector = new Float4Vector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitDoubleArray(GenericArray<Double> array) {
        Field field = FieldAdapter.doubleField(name);
        Float8Vector vector = new Float8Vector(field, allocator);
        VectorHelper.fill(vector, array.values());
        return vector;
    }

    FieldVector visitStringArray(GenericArray<String> stringArray) {
        Field field = FieldAdapter.stringField(name);
        VarCharVector vector = new VarCharVector(field, allocator);
        VectorHelper.fill(vector, stringArray.values());
        return vector;
    }

    FieldVector visitInstantArray(GenericArray<Instant> instantArray) {
        Field field = FieldAdapter.instantField(name);
        TimeStampNanoTZVector vector = new TimeStampNanoTZVector(field, allocator);
        VectorHelper.fill(vector, instantArray.values());
        return vector;
    }
}
