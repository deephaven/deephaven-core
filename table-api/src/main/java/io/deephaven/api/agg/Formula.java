//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.Strings;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An {@link Aggregation aggregation} that provides a single output column that is computed by applying a formula to a
 * set of input columns.
 */
@Immutable
@SimpleStyle
public abstract class Formula implements Aggregation {

    public static Formula parse(String formulaString) {
        return ImmutableFormula.of(Selectable.parse(formulaString));
    }

    public static Formula of(String name, String formula) {
        return of(ColumnName.of(name), formula);
    }

    public static Formula of(ColumnName name, String formula) {
        return of(Selectable.of(name, RawString.of(formula)));
    }

    public static Formula of(Selectable selectable) {
        return ImmutableFormula.of(selectable);
    }

    @Parameter
    public abstract Selectable selectable();

    public ColumnName column() {
        return selectable().newColumn();
    }

    public Expression expression() {
        return selectable().expression();
    }

    /**
     * Return this {@link Formula} as a string of the form {@code <newColumn>=<expression>}
     */
    public String formulaString() {
        return Strings.of(selectable());
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
