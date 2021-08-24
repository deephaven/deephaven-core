package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link String} type.
 */
@Immutable
@SimpleStyle
public abstract class StringType extends GenericTypeBase<String> {

    public static StringType instance() {
        return ImmutableStringType.of();
    }

    @Override
    public final Class<String> clazz() {
        return String.class;
    }

    @Override
    public final <V extends GenericType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        return StringType.class.getName();
    }
}
