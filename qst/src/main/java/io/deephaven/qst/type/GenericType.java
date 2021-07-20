package io.deephaven.qst.type;

public interface GenericType<T> extends Type<T> {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(StringType stringType);

        void visit(InstantType instantType);

        void visit(CustomType<?> customType);
    }
}
