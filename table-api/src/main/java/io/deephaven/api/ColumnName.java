/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.util.NameValidator;
import io.deephaven.api.value.Value;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents a column name.
 */
@Immutable
@SimpleStyle
public abstract class ColumnName
        implements Selectable, Value, Expression, Pair, JoinMatch, JoinAddition, Serializable {

    public static boolean isValidParsedColumnName(String value) {
        return NameValidator.isValidColumnName(value.trim());
    }

    public static ColumnName of(String name) {
        return ImmutableColumnName.of(name);
    }

    public static ColumnName parse(String value) {
        return of(value.trim());
    }

    public static List<ColumnName> from(String... values) {
        return Arrays.stream(values).map(ColumnName::of).collect(Collectors.toList());
    }

    public static List<ColumnName> from(Collection<String> values) {
        return values.stream().map(ColumnName::of).collect(Collectors.toList());
    }

    public static List<String> names(Collection<? extends ColumnName> columns) {
        return columns.stream().map(ColumnName::name).collect(Collectors.toList());
    }

    public static Optional<Collection<ColumnName>> cast(Collection<? extends Selectable> columns) {
        for (Selectable column : columns) {
            if (!(column instanceof ColumnName)) {
                return Optional.empty();
            }
        }
        // noinspection unchecked
        return Optional.of((Collection<ColumnName>) columns);
    }

    /**
     * The column name.
     *
     * @return the column name
     */
    @Parameter
    public abstract String name();

    /**
     * Equivalent to {@code SortColumn.asc(this)}.
     *
     * @return the ascending sort column
     * @see SortColumn#asc(ColumnName)
     */
    public final SortColumn asc() {
        return SortColumn.asc(this);
    }

    /**
     * Equivalent to {@code SortColumn.desc(this)}.
     *
     * @return the descending sort column
     * @see SortColumn#desc(ColumnName)
     */
    public final SortColumn desc() {
        return SortColumn.desc(this);
    }

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
        NameValidator.validateColumnName(name());
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

    @Override
    public final ColumnName left() {
        return this;
    }

    @Override
    public final ColumnName right() {
        return this;
    }

    @Override
    public final ColumnName existingColumn() {
        return this;
    }
}
