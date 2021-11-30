package io.deephaven.api.agg.key;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class KeyCountDistinct extends KeyBase {

    public static KeyCountDistinct of() {
        return ImmutableKeyCountDistinct.builder().build();
    }

    public static KeyCountDistinct of(boolean countNulls) {
        return ImmutableKeyCountDistinct.builder().countNulls(countNulls).build();
    }

    @Default
    public boolean countNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
