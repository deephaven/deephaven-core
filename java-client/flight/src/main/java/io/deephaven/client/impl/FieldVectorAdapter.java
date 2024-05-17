//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.qst.column.header.ColumnHeader;
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
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericType.Visitor;
import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import org.apache.arrow.memory.ArrowBuf;
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
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.Field;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collection;
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

    private static UnsupportedOperationException unsupported(Type<?> type) {
        return new UnsupportedOperationException(String.format("Field type '%s' is not supported yet", type));
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
                        return visitBooleanElements(generic.cast(booleanType).values());
                    }

                    @Override
                    public FieldVector visit(BoxedByteType byteType) {
                        return visitByteElements(generic.cast(byteType).values());
                    }

                    @Override
                    public FieldVector visit(BoxedCharType charType) {
                        return visitCharacterElements(generic.cast(charType).values());
                    }

                    @Override
                    public FieldVector visit(BoxedShortType shortType) {
                        return visitShortElements(generic.cast(shortType).values());
                    }

                    @Override
                    public FieldVector visit(BoxedIntType intType) {
                        return visitIntegerElements(generic.cast(intType).values());
                    }

                    @Override
                    public FieldVector visit(BoxedLongType longType) {
                        return visitLongElements(generic.cast(longType).values());
                    }

                    @Override
                    public FieldVector visit(BoxedFloatType floatType) {
                        return visitFloatElements(generic.cast(floatType).values());
                    }

                    @Override
                    public FieldVector visit(BoxedDoubleType doubleType) {
                        return visitDoubleElements(generic.cast(doubleType).values());
                    }
                });
            }

            @Override
            public FieldVector visit(StringType stringType) {
                return visitStringElements(generic.cast(stringType).values());
            }

            @Override
            public FieldVector visit(InstantType instantType) {
                return visitInstantElements(generic.cast(instantType).values());
            }

            @Override
            public FieldVector visit(ArrayType<?, ?> arrayType) {
                return arrayType.walk(new ArrayType.Visitor<FieldVector>() {
                    @Override
                    public FieldVector visit(NativeArrayType<?, ?> nativeArrayType) {
                        return nativeArrayType.componentType().walk(new Type.Visitor<FieldVector>() {
                            @Override
                            public FieldVector visit(PrimitiveType<?> primitiveType) {
                                return primitiveType.walk(new PrimitiveType.Visitor<FieldVector>() {
                                    @Override
                                    public FieldVector visit(BooleanType booleanType) {
                                        return visitBooleanArrayElements(
                                                generic.cast(booleanType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(ByteType byteType) {
                                        return visitByteArrayElements(generic.cast(byteType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(CharType charType) {
                                        return visitCharArrayElements(generic.cast(charType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(ShortType shortType) {
                                        return visitShortArrayElements(generic.cast(shortType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(IntType intType) {
                                        return visitIntArrayElements(generic.cast(intType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(LongType longType) {
                                        return visitLongArrayElements(generic.cast(longType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(FloatType floatType) {
                                        return visitFloatArrayElements(generic.cast(floatType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(DoubleType doubleType) {
                                        return visitDoubleArrayElements(generic.cast(doubleType.arrayType()).values());
                                    }
                                });
                            }

                            @Override
                            public FieldVector visit(GenericType<?> genericType) {
                                return genericType.walk(new GenericType.Visitor<FieldVector>() {
                                    @Override
                                    public FieldVector visit(BoxedType<?> boxedType) {
                                        throw unsupported(boxedType.arrayType());
                                    }

                                    @Override
                                    public FieldVector visit(StringType stringType) {
                                        return visitStringArrayElements(generic.cast(stringType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(InstantType instantType) {
                                        return visitInstantArrayElements(
                                                generic.cast(instantType.arrayType()).values());
                                    }

                                    @Override
                                    public FieldVector visit(ArrayType<?, ?> arrayType) {
                                        throw unsupported(arrayType.arrayType());
                                    }

                                    @Override
                                    public FieldVector visit(CustomType<?> customType) {
                                        throw unsupported(customType.arrayType());
                                    }
                                });
                            }
                        });
                    }

                    @Override
                    public FieldVector visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
                        throw unsupported(vectorPrimitiveType);
                    }

                    @Override
                    public FieldVector visit(GenericVectorType<?, ?> genericVectorType) {
                        throw unsupported(genericVectorType);
                    }
                });
            }

            @Override
            public FieldVector visit(CustomType<?> customType) {
                throw unsupported(customType);
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

    private FieldVector visitBooleanElements(Collection<Boolean> elements) {
        Field field = FieldAdapter.booleanField(name);
        BitVector vector = new BitVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitByteElements(Collection<Byte> elements) {
        Field field = FieldAdapter.byteField(name);
        TinyIntVector vector = new TinyIntVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitCharacterElements(Collection<Character> elements) {
        Field field = FieldAdapter.charField(name);
        UInt2Vector vector = new UInt2Vector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitShortElements(Collection<Short> elements) {
        Field field = FieldAdapter.shortField(name);
        SmallIntVector vector = new SmallIntVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitIntegerElements(Collection<Integer> elements) {
        Field field = FieldAdapter.intField(name);
        IntVector vector = new IntVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitLongElements(Collection<Long> elements) {
        Field field = FieldAdapter.longField(name);
        BigIntVector vector = new BigIntVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitFloatElements(Collection<Float> elements) {
        Field field = FieldAdapter.floatField(name);
        Float4Vector vector = new Float4Vector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitDoubleElements(Collection<Double> elements) {
        Field field = FieldAdapter.doubleField(name);
        Float8Vector vector = new Float8Vector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitStringElements(Collection<String> elements) {
        Field field = FieldAdapter.stringField(name);
        VarCharVector vector = new VarCharVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitInstantElements(Collection<Instant> elements) {
        Field field = FieldAdapter.instantField(name);
        TimeStampNanoTZVector vector = new TimeStampNanoTZVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    // Manually handling array component types is fragile. We should be able to have a much better generic / recursive
    // Writer based solution, although using the Vector / Writer APIs successfully is non-obvious. In the cases below,
    // we don't have to deal with recursive list types which makes things a bit simpler; although, notice that we must
    // manually advance the position after writeNull because it doesn't automatically advanced the position like endList
    // does.

    private FieldVector visitBooleanArrayElements(Collection<boolean[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.booleanType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        final UnionListWriter writer = new UnionListWriter(vector);
        for (boolean[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (boolean x : array) {
                    writer.writeBit(x ? 1 : 0);
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitByteArrayElements(Collection<byte[]> elements) {
        // Note: byte[] is the only array type that doesn't follow the LIST conventions that the other array types have
        // Might want to re-examine this in the future.
        Field field = FieldAdapter.byteVectorField(name);
        VarBinaryVector vector = new VarBinaryVector(field, allocator);
        VectorHelper.fill(vector, elements);
        return vector;
    }

    private FieldVector visitCharArrayElements(Collection<char[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.charType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (char[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (char x : array) {
                    writer.writeUInt2(x);
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitShortArrayElements(Collection<short[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.shortType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (short[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (short x : array) {
                    writer.writeSmallInt(x);
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitIntArrayElements(Collection<int[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.intType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (int[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (int x : array) {
                    writer.writeInt(x);
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitLongArrayElements(Collection<long[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.longType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (long[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (long x : array) {
                    writer.writeBigInt(x);
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitFloatArrayElements(Collection<float[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.floatType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (float[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (float x : array) {
                    writer.writeFloat4(x);
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitDoubleArrayElements(Collection<double[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.doubleType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (double[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (double x : array) {
                    writer.writeFloat8(x);
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitStringArrayElements(Collection<String[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.stringType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (String[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (String x : array) {
                    if (x == null) {
                        writer.writeNull();
                    } else {
                        final byte[] bytes = x.getBytes(StandardCharsets.UTF_8);
                        try (final ArrowBuf buffer = allocator.buffer(bytes.length)) {
                            buffer.writeBytes(bytes);
                            writer.writeVarChar(0, bytes.length, buffer);
                        }
                    }
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }

    private FieldVector visitInstantArrayElements(Collection<Instant[]> elements) {
        final Field field = FieldAdapter.of(ColumnHeader.of(name, Type.instantType().arrayType()));
        final ListVector vector = new ListVector(field.getName(), allocator, field.getFieldType(), null);
        vector.allocateNew();
        final UnionListWriter writer = new UnionListWriter(vector);
        for (Instant[] array : elements) {
            if (array == null) {
                writer.writeNull();
                writer.setPosition(writer.getPosition() + 1);
            } else {
                writer.startList();
                for (Instant x : array) {
                    if (x == null) {
                        writer.writeNull();
                    } else {
                        final long epochSecond = x.getEpochSecond();
                        final int nano = x.getNano();
                        final long epochNano = Math.addExact(Math.multiplyExact(epochSecond, 1_000_000_000L), nano);
                        writer.writeTimeStampNanoTZ(epochNano);
                    }
                }
                writer.endList();
            }
        }
        vector.setValueCount(elements.size());
        return vector;
    }
}
