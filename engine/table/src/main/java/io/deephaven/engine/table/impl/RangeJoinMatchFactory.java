/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.expression.AbstractExpressionFactory;
import io.deephaven.api.expression.ExpressionParser;
import io.deephaven.api.expression.SelectFactoryConstants;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.Table.RangeEndRule;
import io.deephaven.engine.table.Table.RangeStartRule;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.regex.Matcher;

import static io.deephaven.engine.table.Table.RangeEndRule.*;
import static io.deephaven.engine.table.Table.RangeStartRule.*;

/**
 * {@link JoinMatch} Factory that accepts a number of exact match expressions, followed by a range join relative match
 * expression as specified in {@link Table#rangeJoin(Table, Collection, Collection)}, producing a {@link Result} with
 * the match and rule arguments for
 * {@link Table#rangeJoin(Table, Collection, RangeStartRule, RangeEndRule, Collection)}.
 */
public class RangeJoinMatchFactory {

    public static class Result {

        public final JoinMatch[] joinMatches;
        public final RangeStartRule rangeStartRule;
        public final RangeEndRule rangeEndRule;

        private Result(
                @NotNull final JoinMatch[] joinMatches,
                @NotNull final RangeStartRule rangeStartRule,
                @NotNull final RangeEndRule rangeEndRule) {
            this.joinMatches = joinMatches;
            this.rangeStartRule = rangeStartRule;
            this.rangeEndRule = rangeEndRule;
        }
    }

    private static final ExpressionParser<JoinMatch> exactMatchExpressionParser = new ExpressionParser<>();
    static {
        exactMatchExpressionParser.registerFactory(new AbstractExpressionFactory<>(
                SelectFactoryConstants.START_PTRN + '('
                        + SelectFactoryConstants.ID_PTRN
                        + ')' + SelectFactoryConstants.END_PTRN) {
            @Override
            public JoinMatch getExpression(
                    @NotNull final String expression,
                    @NotNull final Matcher matcher,
                    @NotNull final Object... args) {
                final ColumnName bothExactMachColumn = ColumnName.of(matcher.group(1));
                return JoinMatch.of(bothExactMachColumn, bothExactMachColumn);
            }
        });
        exactMatchExpressionParser.registerFactory(new AbstractExpressionFactory<>(
                SelectFactoryConstants.START_PTRN + '('
                        + SelectFactoryConstants.ID_PTRN + ")\\s*==?\\s*("
                        + SelectFactoryConstants.ID_PTRN
                        + ')' + SelectFactoryConstants.END_PTRN) {
            @Override
            public JoinMatch getExpression(
                    @NotNull final String expression,
                    @NotNull final Matcher matcher,
                    @NotNull final Object... args) {
                final ColumnName leftExactMatchColumn = ColumnName.of(matcher.group(1));
                final ColumnName rightExactMatchColumn = ColumnName.of(matcher.group(2));
                return JoinMatch.of(leftExactMatchColumn, rightExactMatchColumn);
            }
        });
    }

    private static final ExpressionParser<Result> rangeMatchExpressionParser = new ExpressionParser<>();
    static {
        rangeMatchExpressionParser.registerFactory(new AbstractExpressionFactory<>(
                SelectFactoryConstants.START_PTRN + "(<-)?\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")\\s*(<=?)\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")\\s*(<=?)\\s*("
                        + SelectFactoryConstants.ID_PTRN
                        + ")\\s*(->)?" + SelectFactoryConstants.END_PTRN) {
            @Override
            public Result getExpression(
                    @NotNull final String expression,
                    @NotNull final Matcher matcher,
                    @NotNull final Object... args) {
                final boolean allowPreceding = !matcher.group(1).isEmpty();
                final ColumnName leftStartColumn = ColumnName.of(matcher.group(2));
                final boolean startAllowsEqual = matcher.group(3).length() == 2;
                final ColumnName rightRangeColumn = ColumnName.of(matcher.group(4));
                final boolean endAllowsEqual = matcher.group(5).length() == 2;
                final ColumnName leftEndColumn = ColumnName.of(matcher.group(6));
                final boolean allowFollowing = !matcher.group(7).isEmpty();

                if (allowPreceding && !startAllowsEqual) {
                    throw new IllegalArgumentException("Error parsing expression " + expression +
                            ": <- operator for \"allow preceding\" may only be combined with <= start match expression");
                }
                if (allowFollowing && !endAllowsEqual) {
                    throw new IllegalArgumentException("Error parsing expression " + expression +
                            ": -> operator for \"allow following\" may only be combined with <= end match expression");
                }

                final JoinMatch[] joinMatches = (JoinMatch[]) args[0];
                joinMatches[joinMatches.length - 2] = JoinMatch.of(leftStartColumn, rightRangeColumn);
                joinMatches[joinMatches.length - 1] = JoinMatch.of(leftEndColumn, rightRangeColumn);

                return new Result(
                        joinMatches,
                        allowPreceding
                                ? LESS_THAN_OR_EQUAL_ALLOW_PRECEDING
                                : startAllowsEqual
                                        ? LESS_THAN_OR_EQUAL
                                        : LESS_THAN,
                        allowFollowing
                                ? GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING
                                : endAllowsEqual
                                        ? GREATER_THAN_OR_EQUAL
                                        : GREATER_THAN);
            }
        });
    }

    public static Result parse(@NotNull final Collection<String> columnsToMatch) {
        if (columnsToMatch.isEmpty()) {
            throw new IllegalArgumentException(
                    "No match expressions found; must include at least a range match expression");
        }
        final String[] matchExpressions = columnsToMatch.toArray(String[]::new);
        final int numExactMatches = matchExpressions.length - 1;
        final JoinMatch[] joinMatches = new JoinMatch[numExactMatches + 2];
        for (int emi = 0; emi < numExactMatches; ++emi) {
            joinMatches[emi] = exactMatchExpressionParser.parse(matchExpressions[emi]);
        }
        return rangeMatchExpressionParser.parse(matchExpressions[numExactMatches], (Object) joinMatches);
    }
}
