package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Optional;

@Immutable
@SimpleStyle
public abstract class CustomType<T> extends GenericTypeBase<T> {

    public static <T> CustomType<T> of(Class<T> clazz) {
        return ImmutableCustomType.of(clazz);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Override
    public final <V extends GenericType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkClazz() {
        final Optional<Type<T>> staticType = KnownColumnTypes.findStatic(clazz());
        if (staticType.isPresent()) {
            throw new IllegalArgumentException(
                String.format("Use static type %s instead", staticType.get()));
        }
    }
}
