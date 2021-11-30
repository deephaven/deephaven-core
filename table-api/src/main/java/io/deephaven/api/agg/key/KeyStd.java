package io.deephaven.api.agg.key;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class KeyStd extends KeyEmptyBase {

    public static KeyStd of() {
        return ImmutableKeyStd.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
