package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
abstract class JoinMatchImpl implements JoinMatch {

    public static JoinMatchImpl of(ColumnName left, ColumnName right) {
        return ImmutableJoinMatchImpl.of(left, right);
    }

    @Parameter
    public abstract ColumnName left();

    @Parameter
    public abstract ColumnName right();

    @Check
    final void checkNotSameColumn() {
        if (left().equals(right())) {
            // To make sure that JoinMatch#equals() works as we would expect, we should always use
            // canonical ColumnName when applicable.
            throw new IllegalArgumentException(
                    "Should not construct JoinMatchImpl with left() equal to right(), use the ColumnName directly");
        }
    }
}
