package io.deephaven.api.agg.key;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@SimpleStyle
public abstract class KeySum extends KeyEmptyBase {

    public static KeySum of() {
        return ImmutableKeySum.of();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
