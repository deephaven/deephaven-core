//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.generator.primitivetemplate;

import java.util.Objects;

/**
 * Data describing a primitive type.
 */
public class PrimitiveType {

    private final String primitive;
    private final String boxed;
    private final String vector;
    private final String vectorDirect;
    private final String vectorIterator;
    private final String iteratorNext;
    private final String nullValue;
    private final String maxValue;
    private final String minValue;
    private final ValueType valueType;

    /**
     * Create a primitive type description.
     *
     * @param primitive name of the primitive
     * @param boxed name of the boxed primitive
     * @param vector name of the Vector interface
     * @param vectorDirect name of the concrete Vector wrapper
     * @param vectorIterator name of the Vector iterator
     * @param iteratorNext name of the next method on the Vector iterator
     * @param nullValue null value
     * @param maxValue maximum value
     * @param minValue minimum value
     * @param valueType type of value
     */
    public PrimitiveType(final String primitive, final String boxed, final String vector, final String vectorDirect,
            final String vectorIterator, final String iteratorNext,
            final String nullValue, final String maxValue, final String minValue, final ValueType valueType) {
        this.primitive = primitive;
        this.boxed = boxed;
        this.vector = vector;
        this.vectorDirect = vectorDirect;
        this.vectorIterator = vectorIterator;
        this.iteratorNext = iteratorNext;
        this.nullValue = nullValue;
        this.maxValue = maxValue;
        this.minValue = minValue;
        this.valueType = valueType;
    }

    /**
     * Gets the name of the primitive.
     *
     * @return name of the primitive.
     */
    public String getPrimitive() {
        return primitive;
    }

    /**
     * Gets the name of the boxed primitive.
     *
     * @return name of the boxed primitive.
     */
    public String getBoxed() {
        return boxed;
    }

    /**
     * Gets the Vector interface of the primitive.
     *
     * @return Vector interface of the primitive.
     */
    public String getVector() {
        return vector;
    }

    /**
     * Gets the direct Vector wrapper of the primitive.
     *
     * @return direct Vector wrapper of the primitive.
     */
    public String getVectorDirect() {
        return vectorDirect;
    }

    /**
     * Gets the Vector iterator of the primitive.
     *
     * @return Vector iterator of the primitive.
     */
    public String getVectorIterator() {
        return vectorIterator;
    }

    /**
     * Gets the next method on the Vector iterator of the primitive.
     *
     * @return next method on the Vector iterator of the primitive.
     */
    public String getIteratorNext() {
        return iteratorNext;
    }

    /**
     * Gets the null value of the primitive.
     *
     * @return null value of the primitive.
     */
    public String getNull() {
        return nullValue;
    }

    /**
     * Gets the maximum value of the primitive.
     *
     * @return maximum value of the primitive.
     */
    public String getMaxValue() {
        return maxValue;
    }

    /**
     * Gets the minimum value of the primitive.
     *
     * @return minimum value of the primitive.
     */
    public String getMinValue() {
        return minValue;
    }

    /**
     * Gets the value type of the primitive.
     *
     * @return value type of the primitive.
     */
    public ValueType getValueType() {
        return valueType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof PrimitiveType))
            return false;
        PrimitiveType that = (PrimitiveType) o;
        return Objects.equals(primitive, that.primitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primitive);
    }

    public static PrimitiveType[] primitiveTypes() {
        return new PrimitiveType[] {
                new PrimitiveType("char", "Character",
                        "CharVector", "CharVectorDirect",
                        "CloseablePrimitiveIteratorOfChar", "nextChar",
                        "NULL_CHAR", null, null,
                        ValueType.CHARACTER),
                new PrimitiveType("byte", "Byte",
                        "ByteVector", "ByteVectorDirect",
                        "CloseablePrimitiveIteratorOfByte", "nextByte",
                        "NULL_BYTE", "MAX_BYTE", "MIN_BYTE",
                        ValueType.INTEGER),
                new PrimitiveType("short", "Short",
                        "ShortVector", "ShortVectorDirect",
                        "CloseablePrimitiveIteratorOfShort", "nextShort",
                        "NULL_SHORT", "MAX_SHORT", "MIN_SHORT",
                        ValueType.INTEGER),
                new PrimitiveType("int", "Integer",
                        "IntVector", "IntVectorDirect",
                        "CloseablePrimitiveIteratorOfInt", "nextInt",
                        "NULL_INT", "MAX_INT", "MIN_INT",
                        ValueType.INTEGER),
                new PrimitiveType("long", "Long",
                        "LongVector", "LongVectorDirect",
                        "CloseablePrimitiveIteratorOfLong", "nextLong",
                        "NULL_LONG", "MAX_LONG", "MIN_LONG",
                        ValueType.INTEGER),
                new PrimitiveType("float", "Float",
                        "FloatVector", "FloatVectorDirect",
                        "CloseablePrimitiveIteratorOfFloat", "nextFloat",
                        "NULL_FLOAT", "MAX_FLOAT", "MIN_FLOAT",
                        ValueType.FLOATING_POINT),
                new PrimitiveType("double", "Double",
                        "DoubleVector", "DoubleVectorDirect",
                        "CloseablePrimitiveIteratorOfDouble", "nextDouble",
                        "NULL_DOUBLE", "MAX_DOUBLE", "MIN_DOUBLE",
                        ValueType.FLOATING_POINT),
        };
    }
}
