package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Optional;

/**
 * A custom type {@link #clazz() class}.
 *
 * <p>
 * The {@link #clazz() class} must not be representable by a {@link Type#knownTypes() known type},
 * and must not be an array.
 *
 * @param <T> the type
 */
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
    final void checkNotStatic() {
        final Optional<Type<T>> staticType = TypeHelper.findStatic(clazz());
        if (staticType.isPresent()) {
            throw new IllegalArgumentException(
                String.format("Use static type %s instead", staticType.get()));
        }
    }

    @Check
    final void checkNotArray() {
        if (clazz().isArray()) {
            throw new IllegalArgumentException(String.format(
                "Can't create an array type here, use '%s' instead", NativeArrayType.class));
        }
    }

    @Check
    final void checkNotDbArray() {
        if (clazz().getName().startsWith("io.deephaven.db.tables.dbarrays.Db")) {
            throw new IllegalArgumentException("Can't create DB array types as custom types");
        }
    }
}
