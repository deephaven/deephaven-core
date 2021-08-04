package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
abstract class JoinAdditionImpl implements JoinAddition {

    public static JoinAdditionImpl of(ColumnName newColumn, ColumnName existingColumn) {
        return ImmutableJoinAdditionImpl.of(newColumn, existingColumn);
    }

    @Parameter
    public abstract ColumnName newColumn();

    @Parameter
    public abstract ColumnName existingColumn();

    @Check
    final void checkNotSameColumn() {
        if (newColumn().equals(existingColumn())) {
            // To make sure that JoinAddition#equals() works as we would expect, we should always
            // use canonical ColumnName when applicable.
            throw new IllegalArgumentException(
                "Should not construct JoinAdditionImpl with equal columns, use the ColumnName directly");
        }
    }
}
