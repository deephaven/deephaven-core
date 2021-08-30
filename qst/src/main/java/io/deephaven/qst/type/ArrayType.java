package io.deephaven.qst.type;

/**
 * An array-like type.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 * @see NativeArrayType
 * @see DbPrimitiveArrayType
 * @see DbGenericArrayType
 */
public interface ArrayType<T, ComponentType> extends GenericType<T> {

    /**
     * The component type.
     *
     * @return the component type
     */
    Type<ComponentType> componentType();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(NativeArrayType<?, ?> nativeArrayType);

        void visit(DbPrimitiveArrayType<?, ?> dbArrayPrimitiveType);

        void visit(DbGenericArrayType<?, ?> dbGenericArrayType);
    }
}
