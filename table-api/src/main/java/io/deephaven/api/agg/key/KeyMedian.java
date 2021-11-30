package io.deephaven.api.agg.key;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@BuildableStyle
public abstract class KeyMedian extends KeyBase {

    public static KeyMedian of() {
        return ImmutableKeyMedian.builder().build();
    }

    public static KeyMedian of(boolean averageMedian) {
        return ImmutableKeyMedian.builder().averageMedian(averageMedian).build();
    }

    @Default
    public boolean averageMedian() {
        return true;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
