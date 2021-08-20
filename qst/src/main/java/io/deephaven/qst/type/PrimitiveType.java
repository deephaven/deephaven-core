package io.deephaven.qst.type;

/**
 * A primitive type.
 *
 * @param <T> the primitive type
 * @see BooleanType
 * @see ByteType
 * @see CharType
 * @see ShortType
 * @see IntType
 * @see LongType
 * @see FloatType
 * @see DoubleType
 */
public interface PrimitiveType<T> extends Type<T> {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(BooleanType booleanType);

        void visit(ByteType byteType);

        void visit(CharType charType);

        void visit(ShortType shortType);

        void visit(IntType intType);

        void visit(LongType longType);

        void visit(FloatType floatType);

        void visit(DoubleType doubleType);
    }
}
