package io.deephaven.qst.type;

/**
 * A non-primitive object type.
 *
 * @param <T> the object type
 * @see StringType
 * @see InstantType
 * @see CustomType
 */
public interface GenericType<T> extends Type<T> {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(StringType stringType);

        void visit(InstantType instantType);

        // Implementation note: when adding new types here, add type to TypeHelper

        void visit(CustomType<?> customType);
    }
}
