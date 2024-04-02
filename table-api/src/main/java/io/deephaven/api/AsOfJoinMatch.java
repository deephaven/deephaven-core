//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.deephaven.api.expression.SelectFactoryConstants.END_PTRN;
import static io.deephaven.api.expression.SelectFactoryConstants.ID_PTRN;
import static io.deephaven.api.expression.SelectFactoryConstants.START_PTRN;


@Immutable
@SimpleStyle
public abstract class AsOfJoinMatch {

    private static final Pattern EXPRESSION_PATTERN =
            Pattern.compile(START_PTRN + "(" + ID_PTRN + ")\\s*(>=?|<=?)\\s*(" + ID_PTRN + ")" + END_PTRN);

    public static AsOfJoinMatch of(
            final ColumnName leftColumn,
            final AsOfJoinRule joinRule,
            final ColumnName rightColumn) {
        return ImmutableAsOfJoinMatch.of(leftColumn, joinRule, rightColumn);
    }

    /**
     * Parses the expression {@code x}, expecting it to either be a single column name, or an expression of the form
     * {@code "lhsColumnName >= rhsColumnName"} or {@code "lhsColumnName > rhsColumnName"}. When it is a single column
     * name, a join rule {@link AsOfJoinRule#GREATER_THAN_EQUAL} is used.
     *
     * @param x the expression
     * @return the as-of join match
     */
    public static AsOfJoinMatch parseForAj(String x) {
        try {
            final ColumnName columnName = ColumnName.parse(x);
            return of(columnName, AsOfJoinRule.GREATER_THAN_EQUAL, columnName);
        } catch (IllegalArgumentException e) {
            // ignore, try and parse
        }
        final AsOfJoinMatch parsed = parse(x);
        switch (parsed.joinRule()) {
            case GREATER_THAN:
            case GREATER_THAN_EQUAL:
                return parsed;
        }
        throw new IllegalArgumentException("Unsupported operation for aj: " + parsed.joinRule());
    }

    /**
     * Parses the expression {@code x}, expecting it to either be a single column name, or an expression of the form
     * {@code "lhsColumnName <= rhsColumnName"} or {@code "lhsColumnName < rhsColumnName"}. When it is a single column
     * name, a join rule {@link AsOfJoinRule#LESS_THAN_EQUAL} is used.
     *
     * @param x the expression
     * @return the as-of join match
     */
    public static AsOfJoinMatch parseForRaj(String x) {
        try {
            final ColumnName columnName = ColumnName.parse(x);
            return of(columnName, AsOfJoinRule.LESS_THAN_EQUAL, columnName);
        } catch (IllegalArgumentException e) {
            // ignore, try and parse
        }
        final AsOfJoinMatch parse = parse(x);
        switch (parse.joinRule()) {
            case LESS_THAN:
            case LESS_THAN_EQUAL:
                return parse;
        }
        throw new IllegalArgumentException("Unsupported operation for raj: " + parse.joinRule());
    }

    static AsOfJoinMatch parse(String x) {
        final Matcher matcher = EXPRESSION_PATTERN.matcher(x);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format(
                    "Input expression %s does not match expected pattern %s",
                    x, EXPRESSION_PATTERN.pattern()));
        }
        return of(
                ColumnName.of(matcher.group(1)),
                AsOfJoinRule.parse(matcher.group(2)),
                ColumnName.of(matcher.group(3)));
    }

    @Parameter
    public abstract ColumnName leftColumn();

    @Parameter
    public abstract AsOfJoinRule joinRule();

    @Parameter
    public abstract ColumnName rightColumn();

    public final String toRpcString() {
        return leftColumn().name() + joinRule().operatorString() + rightColumn().name();
    }

    public final boolean isAj() {
        switch (joinRule()) {
            case GREATER_THAN_EQUAL:
            case GREATER_THAN:
                return true;
        }
        return false;
    }

    public final boolean isRaj() {
        switch (joinRule()) {
            case LESS_THAN_EQUAL:
            case LESS_THAN:
                return true;
        }
        return false;
    }
}
