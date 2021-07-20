package io.deephaven.qst.type;

public interface PrimitiveType<T> extends Type<T> {

    Class<T> primitiveClass();

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
