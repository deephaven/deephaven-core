package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.Serializable;

@Immutable
@SimpleStyle
abstract class SelectableImpl implements Selectable, Serializable {

    public static SelectableImpl of(ColumnName newColumn, Expression expression) {
        return ImmutableSelectableImpl.of(newColumn, expression);
    }

    @Parameter
    public abstract ColumnName newColumn();

    @Parameter
    public abstract Expression expression();

    @Check
    final void checkExpressionNotSameColumn() {
        if (expression().equals(newColumn())) {
            // To make sure that Selectable#equals() works as we would expect, we should always use
            // canonical ColumnName when applicable.
            throw new IllegalArgumentException(
                    "Should not construct SelectableImpl with expression() equal to newColumn(), use the ColumnName directly");
        }
    }
}
