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
    private final String posInf;
    private final String negInf;
    private final ValueType valueType;

    /**
     * Create a primitive type description.
     *
     * @param primitive name of the primitive
     * @param boxed name of the boxed primitive
     * @param dbArray name of the DbArray interface
     * @param dbArrayDirect name of the concrete DbArray wrapper
     * @param nullValue null value
     * @param posInf maximum value
     * @param negInf minimum value
     * @param valueType type of value
     */
    public PrimitiveType(final String primitive, final String boxed, final String dbArray, final String dbArrayDirect, final String nullValue, final String posInf, final String negInf, final ValueType valueType) {
        this.primitive = primitive;
        this.boxed = boxed;
        this.dbArray = dbArray;
        this.dbArrayDirect = dbArrayDirect;
        this.nullValue = nullValue;
        this.posInf = posInf;
        this.negInf = negInf;
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
    public String getPosInf() {
        return posInf;
    }

    /**
     * Gets the minimum value of the primitive.
     *
     * @return minimum value of the primitive.
     */
    public String getNegInf() {
        return negInf;
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
        if (this == o) return true;
        if (!(o instanceof PrimitiveType)) return false;
        PrimitiveType that = (PrimitiveType) o;
        return Objects.equals(primitive, that.primitive);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primitive);
    }

    public static PrimitiveType[] primitiveTypes() {
        return new PrimitiveType[] {
                new PrimitiveType("Boolean", "Boolean", "BooleanVector", "BooleanVectorDirect", "NULL_BOOLEAN", null, null, ValueType.BOOLEAN),
                new PrimitiveType("char", "Character", "CharVector", "CharVectorDirect", "NULL_CHAR", null, null, ValueType.CHARACTER),
                new PrimitiveType("byte", "Byte", "ByteVector", "ByteVectorDirect","NULL_BYTE", "POS_INF_BYTE", "NEG_INF_BYTE", ValueType.INTEGER),
                new PrimitiveType("short", "Short", "ShortVector","ShortVectorDirect", "NULL_SHORT", "POS_INF_SHORT", "NEG_INF_SHORT", ValueType.INTEGER),
                new PrimitiveType("int", "Integer", "IntVector", "IntVectorDirect","NULL_INT", "POS_INF_INT", "NEG_INF_INT", ValueType.INTEGER),
                new PrimitiveType("long", "Long", "LongVector", "LongVectorDirect","NULL_LONG", "POS_INF_LONG", "NEG_INF_LONG", ValueType.INTEGER),
                new PrimitiveType("float", "Float", "FloatVector", "FloatVectorDirect","NULL_FLOAT", "POS_INF_FLOAT", "NEG_INF_FLOAT",ValueType.FLOATING_POINT),
                new PrimitiveType("double", "Double", "DoubleVector", "DoubleVectorDirect","NULL_DOUBLE", "POS_INF_DOUBLE", "NEG_INF_DOUBLE", ValueType.FLOATING_POINT),
        };
    }
}
