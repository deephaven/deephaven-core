package io.deephaven.api;

import io.deephaven.api.agg.Pair;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.value.Value;
import io.deephaven.db.tables.utils.DBNameValidator;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.Serializable;

/**
 * Represents a column name.
 */
@Immutable
@SimpleStyle
public abstract class ColumnName implements Selectable, Value, Expression, Pair, Serializable {

    public static boolean isValidColumnName(String name) {
        try {
            DBNameValidator.validateColumnName(name);
            return true;
        } catch (DBNameValidator.InvalidNameException e) {
            return false;
        }
    }

    public static boolean isValidParsedColumnName(String value) {
        return isValidColumnName(value.trim());
    }

    public static ColumnName of(String name) {
        return ImmutableColumnName.of(name);
    }

    public static ColumnName parse(String value) {
        return of(value.trim());
    }

    /**
     * The column name.
     *
     * @return the column name
     */
    @Parameter
    public abstract String name();

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends Value.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkName() {
        DBNameValidator.validateColumnName(name());
    }

    @Override
    public final ColumnName newColumn() {
        return this;
    }

    @Override
    public final Expression expression() {
        return this;
    }

    @Override
    public final ColumnName input() {
        return this;
    }

    @Override
    public final ColumnName output() {
        return this;
    }
}
