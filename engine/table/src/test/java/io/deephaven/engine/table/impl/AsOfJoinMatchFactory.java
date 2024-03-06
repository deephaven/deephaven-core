//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.AsOfJoinMatch;
import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.TableOperationsDefaults;
import io.deephaven.api.expression.AbstractExpressionFactory;
import io.deephaven.api.expression.ExpressionParser;
import io.deephaven.api.expression.SelectFactoryConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.regex.Matcher;

/**
 * {@link JoinMatch} Factory that accepts final value of either =, &lt;=, or &lt;, &gt; &gt;= and returns a JoinMatch[]
 * and either an {@link AsOfJoinRule}.
 *
 * <p>
 * Deprecated: replace with {@link TableOperationsDefaults#splitToList(String)}, {@link JoinMatch#from(String...)},
 * {@link AsOfJoinMatch#parseForAj(String)}, and/or {@link AsOfJoinMatch#parseForRaj(String)}.
 */
@Deprecated
public class AsOfJoinMatchFactory {
    public static class AsOfJoinResult {
        public final List<JoinMatch> matches;
        public final AsOfJoinMatch joinMatch;

        private AsOfJoinResult(List<JoinMatch> matches, AsOfJoinMatch joinMatch) {
            this.matches = Objects.requireNonNull(matches);
            this.joinMatch = Objects.requireNonNull(joinMatch);
        }
    }

    private static final ExpressionParser<AsOfJoinMatch> asOfJoinParser = new ExpressionParser<>();
    private static final ExpressionParser<AsOfJoinMatch> reverseAsOfJoinParser = new ExpressionParser<>();
    static {
        final BiFunction<JoinMatch, AsOfJoinRule, AsOfJoinMatch> bif =
                (joinMatch, joinRule) -> AsOfJoinMatch.of(joinMatch.left(), joinRule, joinMatch.right());
        registerSingularFactory(asOfJoinParser, AsOfJoinRule.GREATER_THAN_EQUAL, bif);
        asOfJoinParser.registerFactory(new AbstractExpressionFactory<AsOfJoinMatch>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*>=\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public AsOfJoinMatch getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return AsOfJoinMatch.of(firstColumn, AsOfJoinRule.GREATER_THAN_EQUAL, secondColumn);
            }
        });
        asOfJoinParser.registerFactory(new AbstractExpressionFactory<AsOfJoinMatch>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*>\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public AsOfJoinMatch getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return AsOfJoinMatch.of(firstColumn, AsOfJoinRule.GREATER_THAN, secondColumn);
            }
        });

        registerSingularFactory(reverseAsOfJoinParser, AsOfJoinRule.LESS_THAN_EQUAL, bif);
        reverseAsOfJoinParser.registerFactory(new AbstractExpressionFactory<AsOfJoinMatch>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*<=\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public AsOfJoinMatch getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return AsOfJoinMatch.of(firstColumn, AsOfJoinRule.LESS_THAN_EQUAL, secondColumn);
            }
        });
        reverseAsOfJoinParser.registerFactory(new AbstractExpressionFactory<AsOfJoinMatch>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*<\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public AsOfJoinMatch getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return AsOfJoinMatch.of(firstColumn, AsOfJoinRule.LESS_THAN, secondColumn);
            }
        });
    }

    private static <T, R> void registerSingularFactory(
            ExpressionParser<T> joinParser, R rule, BiFunction<JoinMatch, R, T> factory) {
        joinParser.registerFactory(new AbstractExpressionFactory<T>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")"
                        + SelectFactoryConstants.END_PTRN) {
            @Override
            public T getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName column = ColumnName.of(matcher.group(1));
                return factory.apply(JoinMatch.of(column, column), rule);
            }
        });
    }

    private static AsOfJoinMatch getAjExpression(String match) {
        return asOfJoinParser.parse(match);
    }

    public static AsOfJoinResult getAjExpressions(String... matches) {
        final List<JoinMatch> result = new ArrayList<>(matches.length - 1);
        for (int ii = 0; ii < matches.length - 1; ++ii) {
            result.add(JoinMatch.parse(matches[ii]));
        }
        final AsOfJoinMatch joinMatch = getAjExpression(matches[matches.length - 1]);
        return new AsOfJoinResult(Collections.unmodifiableList(result), joinMatch);
    }

    public static AsOfJoinResult getAjExpressions(Collection<String> matches) {
        return getAjExpressions(matches.toArray(new String[0]));
    }

    private static AsOfJoinMatch getRajExpression(String match) {
        return reverseAsOfJoinParser.parse(match);
    }

    public static AsOfJoinResult getRajExpressions(String... matches) {
        final List<JoinMatch> result = new ArrayList<>(matches.length - 1);
        for (int ii = 0; ii < matches.length - 1; ++ii) {
            result.add(JoinMatch.parse(matches[ii]));
        }
        final AsOfJoinMatch joinMatch = getRajExpression(matches[matches.length - 1]);
        return new AsOfJoinResult(Collections.unmodifiableList(result), joinMatch);
    }

    public static AsOfJoinResult getRajExpressions(Collection<String> matches) {
        return getRajExpressions(matches.toArray(new String[0]));
    }
}
