package io.deephaven.api;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

@Immutable(builder = false, copy = false)
public abstract class ColumnName
    implements JoinMatch, JoinAddition, Selectable, Expression, Filter {

    private final static Pattern COLUMN_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");
    private final static Set<String> RESERVED;
    static {
        final Set<String> reserved = new HashSet<>();
        reserved.add("i"); // todo, add others
        RESERVED = Collections.unmodifiableSet(reserved);
    }

    public static boolean isValidColumnName(String name) {
        return !RESERVED.contains(name) && COLUMN_NAME_PATTERN.matcher(name).matches();
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
        if (!isValidColumnName(name())) {
            throw new IllegalArgumentException(String.format("Invalid column name: '%s'", name()));
        }
    }
}
