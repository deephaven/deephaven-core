package io.deephaven.qst.type;

import io.deephaven.qst.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class StringType extends GenericTypeBase<String> {

    public static StringType instance() {
        return ImmutableStringType.of();
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
