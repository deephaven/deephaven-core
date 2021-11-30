package io.deephaven.api.agg.key;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@BuildableStyle
public abstract class KeyUnique extends KeyBase {

    public static KeyUnique of() {
        return ImmutableKeyUnique.builder().build();
    }

    public static KeyUnique of(boolean includeNulls) {
        return ImmutableKeyUnique.builder().includeNulls(includeNulls).build();
    }

    @Default
    public boolean includeNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
