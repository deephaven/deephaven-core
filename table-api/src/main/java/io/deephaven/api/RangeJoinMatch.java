/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.deephaven.api.RangeEndRule.*;
import static io.deephaven.api.RangeStartRule.*;
import static io.deephaven.api.expression.SelectFactoryConstants.END_PTRN;
import static io.deephaven.api.expression.SelectFactoryConstants.ID_PTRN;
import static io.deephaven.api.expression.SelectFactoryConstants.START_PTRN;

/**
 * A RangeJoinMatch specifies the columns and relationships used to determine a bucket's responsive rows from the right
 * table for each row in the left table of a range join.
 */
@Immutable
@BuildableStyle
public abstract class RangeJoinMatch implements Serializable {

    public static Builder builder() {
        return ImmutableRangeJoinMatch.builder();
    }

    public static RangeJoinMatch of(
            final ColumnName leftStartColumn,
            final RangeStartRule rangeStartRule,
            final ColumnName rightRangeColumn,
            final RangeEndRule rangeEndRule,
            final ColumnName leftEndColumn) {
        return builder()
                .leftStartColumn(leftStartColumn)
                .rangeStartRule(rangeStartRule)
                .rightRangeColumn(rightRangeColumn)
                .rangeEndRule(rangeEndRule)
                .leftEndColumn(leftEndColumn)
                .build();
    }

    private static final Pattern EXPRESSION_PATTERN = Pattern.compile(
            START_PTRN + "(<-)?\\s*("
                    + ID_PTRN + ")\\s*(<=?)\\s*("
                    + ID_PTRN + ")\\s*(<=?)\\s*("
                    + ID_PTRN
                    + ")\\s*(->)?" + END_PTRN);

    public static RangeJoinMatch parse(final String input) {
        final Matcher matcher = EXPRESSION_PATTERN.matcher(input);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format(
                    "Input expression %s does not match expected pattern %s",
                    input, EXPRESSION_PATTERN.pattern()));
        }

        final boolean allowPreceding = !matcher.group(1).isEmpty();
        final ColumnName leftStartColumn = ColumnName.of(matcher.group(2));
        final boolean startAllowsEqual = matcher.group(3).length() == 2;
        final ColumnName rightRangeColumn = ColumnName.of(matcher.group(4));
        final boolean endAllowsEqual = matcher.group(5).length() == 2;
        final ColumnName leftEndColumn = ColumnName.of(matcher.group(6));
        final boolean allowFollowing = !matcher.group(7).isEmpty();

        if (allowPreceding && !startAllowsEqual) {
            throw new IllegalArgumentException("Error parsing expression " + input +
                    ": <- operator for \"allow preceding\" may only be combined with <= start match expression");
        }
        final RangeStartRule rangeStartRule = allowPreceding
                ? LESS_THAN_OR_EQUAL_ALLOW_PRECEDING
                : startAllowsEqual
                        ? LESS_THAN_OR_EQUAL
                        : LESS_THAN;

        if (allowFollowing && !endAllowsEqual) {
            throw new IllegalArgumentException("Error parsing expression " + input +
                    ": -> operator for \"allow following\" may only be combined with <= end match expression");
        }
        final RangeEndRule rangeEndRule = allowFollowing
                ? GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING
                : endAllowsEqual
                        ? GREATER_THAN_OR_EQUAL
                        : GREATER_THAN;

        return of(leftStartColumn, rangeStartRule, rightRangeColumn, rangeEndRule, leftEndColumn);
    }

    /**
     * The column from the left table that bounds the start of the responsive range from the right table.
     *
     * @return The left start column name
     */
    public abstract ColumnName leftStartColumn();

    /**
     * The rule applied to {@link #leftStartColumn()} and {@link #rightRangeColumn()} to determine the start of the
     * responsive range from the right table for a given left table row.
     * 
     * @return The range start rule
     */
    public abstract RangeStartRule rangeStartRule();

    /**
     * The column name from the right table that determines which right table rows are responsive to a given left table
     * row.
     * 
     * @return The right range column name
     */
    public abstract ColumnName rightRangeColumn();

    /**
     * The rule applied to {@link #leftStartColumn()} and {@link #rightRangeColumn()} to determine the end of the
     * responsive range from the right table for a given left table row.
     * 
     * @return The range end rule
     */
    public abstract RangeEndRule rangeEndRule();

    /**
     * The column from the left table that bounds the end of the responsive range from the right table.
     *
     * @return The left end column name
     */
    public abstract ColumnName leftEndColumn();

    @Check
    public final void checkLeftColumnsDifferent() {
        if (leftStartColumn().equals(leftEndColumn())) {
            throw new IllegalArgumentException(String.format(
                    "RangeJoinMatch must have distinct ColumnNames for leftStartColumn() and leftEndColumn() in order to properly define a range, %s specified for both fields",
                    leftStartColumn().name()));
        }
    }

    public interface Builder {

        Builder leftStartColumn(ColumnName leftStartColumn);

        Builder rangeStartRule(RangeStartRule rangeStartRule);

        Builder rightRangeColumn(ColumnName rightRangeColumn);

        Builder rangeEndRule(RangeEndRule rangeEndRule);

        Builder leftEndColumn(ColumnName leftEndColumn);

        RangeJoinMatch build();
    }
}
