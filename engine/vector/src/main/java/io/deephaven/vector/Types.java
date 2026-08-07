//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.ShortType;

final class Types {
    public static final PrimitiveVectorType<ByteVector, Byte> BYTE_VECTOR_TYPE =
            PrimitiveVectorType.of(ByteVector.class, ByteType.of());
    public static final PrimitiveVectorType<CharVector, Character> CHAR_VECTOR_TYPE =
            PrimitiveVectorType.of(CharVector.class, CharType.of());
    public static final PrimitiveVectorType<ShortVector, Short> SHORT_VECTOR_TYPE =
            PrimitiveVectorType.of(ShortVector.class, ShortType.of());
    public static final PrimitiveVectorType<IntVector, Integer> INT_VECTOR_TYPE =
            PrimitiveVectorType.of(IntVector.class, IntType.of());
    public static final PrimitiveVectorType<LongVector, Long> LONG_VECTOR_TYPE =
            PrimitiveVectorType.of(LongVector.class, LongType.of());
    public static final PrimitiveVectorType<FloatVector, Float> FLOAT_VECTOR_TYPE =
            PrimitiveVectorType.of(FloatVector.class, FloatType.of());
    public static final PrimitiveVectorType<DoubleVector, Double> DOUBLE_VECTOR_TYPE =
            PrimitiveVectorType.of(DoubleVector.class, DoubleType.of());

    private Types() {}
}
