package io.deephaven.qst.table;

import java.util.regex.Pattern;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable(builder = false, copy = false)
public abstract class ColumnName
    implements JoinMatch, JoinAddition, Selectable, Expression, Filter {

    // todo: extract DBNameValidator or something similar for column names
    // todo: make better
    private final static Pattern COLUMN_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

    public static boolean isValidColumnName(String name) {
        return COLUMN_NAME_PATTERN.matcher(name).matches();
    }

    public static ColumnName of(String name) {
        return ImmutableColumnName.of(name);
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
        // todo: better validation in shared library
        if (!isValidColumnName(name())) {
            throw new IllegalArgumentException(String.format("Invalid column name: '%s'", name()));
        }
    }
}
