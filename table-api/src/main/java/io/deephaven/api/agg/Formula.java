//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An {@link Aggregation aggregation} that provides a single output column that is computed by applying a formula to a
 * set of input columns.
 *
 * @see io.deephaven.api.TableOperations#countBy
 */
@Immutable
@SimpleStyle
public abstract class Formula implements Aggregation {

    public static Formula of(ColumnName name, String formula) {
        return ImmutableFormula.of(name, formula);
    }

    @Parameter
    public abstract ColumnName column();

    @Parameter
    public abstract String formula();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
