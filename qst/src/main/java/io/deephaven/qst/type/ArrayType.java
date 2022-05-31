package io.deephaven.qst.type;

/**
 * An array-like type.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 * @see NativeArrayType
 * @see PrimitiveVectorType
 * @see GenericVectorType
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

        void visit(PrimitiveVectorType<?, ?> vectorPrimitiveType);

        void visit(GenericVectorType<?, ?> genericVectorType);
    }
}
