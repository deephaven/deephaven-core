/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.generator.primitivetemplate;

import java.util.Objects;

/**
 * Data describing a primitive type.
 */
public class PrimitiveType {

    private final String primitive;
    private final String boxed;
    private final String dbArray;
    private final String dbArrayDirect;
    private final String nullValue;
    private final String maxValue;
    private final String minValue;
    private final ValueType valueType;

    /**
     * Create a primitive type description.
     *
     * @param primitive name of the primitive
     * @param boxed name of the boxed primitive
     * @param dbArray name of the DbArray interface
     * @param dbArrayDirect name of the concrete DbArray wrapper
     * @param nullValue null value
     * @param maxValue maximum value
     * @param minValue minimum value
     * @param valueType type of value
     */
    public PrimitiveType(final String primitive, final String boxed, final String dbArray, final String dbArrayDirect,
            final String nullValue, final String maxValue, final String minValue, final ValueType valueType) {
        this.primitive = primitive;
        this.boxed = boxed;
        this.dbArray = dbArray;
        this.dbArrayDirect = dbArrayDirect;
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
     * Gets the DbArray interface of the primitive.
     *
     * @return DbArray interface of the primitive.
     */
    public String getDbArray() {
        return dbArray;
    }

    /**
     * Gets the direct DbArray wrapper of the primitive.
     *
     * @return direct DbArray wrapper of the primitive.
     */
    public String getDbArrayDirect() {
        return dbArrayDirect;
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
                new PrimitiveType("Boolean", "Boolean",
                        "BooleanVector", "BooleanVectorDirect",
                        "NULL_BOOLEAN", null, null,
                        ValueType.BOOLEAN),
                new PrimitiveType("char", "Character",
                        "CharVector", "CharVectorDirect",
                        "NULL_CHAR", null, null,
                        ValueType.CHARACTER),
                new PrimitiveType("byte", "Byte",
                        "ByteVector", "ByteVectorDirect",
                        "NULL_BYTE", "MAX_BYTE", "MIN_BYTE",
                        ValueType.INTEGER),
                new PrimitiveType("short", "Short",
                        "ShortVector", "ShortVectorDirect",
                        "NULL_SHORT", "MAX_SHORT", "MIN_SHORT",
                        ValueType.INTEGER),
                new PrimitiveType("int", "Integer",
                        "IntVector", "IntVectorDirect",
                        "NULL_INT", "MAX_INT", "MIN_INT",
                        ValueType.INTEGER),
                new PrimitiveType("long", "Long",
                        "LongVector", "LongVectorDirect",
                        "NULL_LONG", "MAX_LONG", "MIN_LONG",
                        ValueType.INTEGER),
                new PrimitiveType("float", "Float",
                        "FloatVector", "FloatVectorDirect",
                        "NULL_FLOAT", "MAX_FLOAT", "MIN_FLOAT",
                        ValueType.FLOATING_POINT),
                new PrimitiveType("double", "Double",
                        "DoubleVector", "DoubleVectorDirect",
                        "NULL_DOUBLE", "MAX_DOUBLE", "MIN_DOUBLE",
                        ValueType.FLOATING_POINT),
        };
    }
}
