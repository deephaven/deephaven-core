package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An {@link Aggregation aggregation} that provides a single output column with the last row key from the input table
 * for each aggregation group in the result.
 * <p>
 * The primary use case for this aggregation is to allow for a subsequent {@link io.deephaven.api.TableOperations#sort
 * sort} on the output column to order aggregated data by current last occurrence in the input table rather than
 * encounter order.
 */
@Immutable
@SimpleStyle
public abstract class LastRowKey implements Aggregation {

    public static LastRowKey of(ColumnName name) {
        return ImmutableLastRowKey.of(name);
    }

    public static LastRowKey of(String x) {
        return of(ColumnName.of(x));
    }

    @Parameter
    public abstract ColumnName column();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
