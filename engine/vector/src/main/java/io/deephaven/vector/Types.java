/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.Type;

class Types {
    static final PrimitiveVectorType<ByteVector, Byte> BYTE = PrimitiveVectorType.of(ByteVector.class, Type.byteType());
    static final PrimitiveVectorType<CharVector, Character> CHAR =
            PrimitiveVectorType.of(CharVector.class, Type.charType());
    static final PrimitiveVectorType<ShortVector, Short> SHORT =
            PrimitiveVectorType.of(ShortVector.class, Type.shortType());
    static final PrimitiveVectorType<IntVector, Integer> INT = PrimitiveVectorType.of(IntVector.class, Type.intType());
    static final PrimitiveVectorType<LongVector, Long> LONG = PrimitiveVectorType.of(LongVector.class, Type.longType());
    static final PrimitiveVectorType<FloatVector, Float> FLOAT =
            PrimitiveVectorType.of(FloatVector.class, Type.floatType());
    static final PrimitiveVectorType<DoubleVector, Double> DOUBLE =
            PrimitiveVectorType.of(DoubleVector.class, Type.doubleType());
}
