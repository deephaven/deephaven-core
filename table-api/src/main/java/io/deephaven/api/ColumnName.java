package io.deephaven.api;

import io.deephaven.db.tables.utils.DBNameValidator;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnName
    implements JoinMatch, JoinAddition, Selectable, Expression, Filter {

    public static boolean isValidColumnName(String name) {
        try {
            DBNameValidator.validateColumnName(name);
            return true;
        } catch (DBNameValidator.InvalidNameException e) {
            return false;
        }
    }

    public static ColumnName of(String name) {
        return ImmutableColumnName.of(name);
    }

    public static boolean isValidParsedColumnName(String value) {
        return isValidColumnName(value.trim());
    }

    public static ColumnName parse(String value) {
        return of(value.trim());
    }

    @Parameter
    public abstract String name();

    @Override
    public final <V extends JoinMatch.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends JoinAddition.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends Selectable.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends Filter.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkName() {
        DBNameValidator.validateColumnName(name());
    }
}
