package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class GenericVectorType<T, ComponentType> extends ArrayTypeBase<T, ComponentType> {

    public static <T, ComponentType> GenericVectorType<T, ComponentType> of(Class<T> clazz,
            GenericType<ComponentType> genericType) {
        return ImmutableGenericVectorType.of(clazz, genericType);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Parameter
    public abstract GenericType<ComponentType> componentType();

    @Override
    public final <V extends ArrayType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
