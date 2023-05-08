/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.ReverseAsOfJoinRule;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.regex.Matcher;

/**
 * {@link JoinMatch} Factory that accepts final value of either =, &lt;=, or &lt;, &gt; &gt;= and returns a JoinMatch[]
 * and either an {@link AsOfJoinRule} or a {@link ReverseAsOfJoinRule}.
 */
public class AsOfJoinMatchFactory {
    public static class AsOfJoinResult {
        public final JoinMatch[] matches;
        public final AsOfJoinRule rule;

        private AsOfJoinResult(JoinMatch[] matches, AsOfJoinRule rule) {
            this.matches = matches;
            this.rule = rule;
        }

        private AsOfJoinResult(JoinMatch match, AsOfJoinRule rule) {
            this.matches = new JoinMatch[] {match};
            this.rule = rule;
        }
    }

    public static class ReverseAsOfJoinResult {
        public final JoinMatch[] matches;
        public final ReverseAsOfJoinRule rule;

        private ReverseAsOfJoinResult(JoinMatch[] matches, ReverseAsOfJoinRule rule) {
            this.matches = matches;
            this.rule = rule;
        }

        private ReverseAsOfJoinResult(JoinMatch match, ReverseAsOfJoinRule rule) {
            this.matches = new JoinMatch[] {match};
            this.rule = rule;
        }
    }

    private static final ExpressionParser<AsOfJoinResult> asOfJoinParser = new ExpressionParser<>();
    private static final ExpressionParser<ReverseAsOfJoinResult> reverseAsOfJoinParser = new ExpressionParser<>();
    static {
        registerEqualFactories(asOfJoinParser, AsOfJoinRule.LESS_THAN_EQUAL, AsOfJoinResult::new);
        asOfJoinParser.registerFactory(new AbstractExpressionFactory<AsOfJoinResult>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*<=\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public AsOfJoinResult getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return new AsOfJoinResult(JoinMatch.of(firstColumn, secondColumn), AsOfJoinRule.LESS_THAN_EQUAL);
            }
        });
        asOfJoinParser.registerFactory(new AbstractExpressionFactory<AsOfJoinResult>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*<\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public AsOfJoinResult getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return new AsOfJoinResult(JoinMatch.of(firstColumn, secondColumn), AsOfJoinRule.LESS_THAN);
            }
        });

        registerEqualFactories(reverseAsOfJoinParser, ReverseAsOfJoinRule.GREATER_THAN_EQUAL,
                ReverseAsOfJoinResult::new);
        reverseAsOfJoinParser.registerFactory(new AbstractExpressionFactory<ReverseAsOfJoinResult>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*>=\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public ReverseAsOfJoinResult getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return new ReverseAsOfJoinResult(JoinMatch.of(firstColumn, secondColumn),
                        ReverseAsOfJoinRule.GREATER_THAN_EQUAL);
            }
        });
        reverseAsOfJoinParser.registerFactory(new AbstractExpressionFactory<ReverseAsOfJoinResult>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*>\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public ReverseAsOfJoinResult getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return new ReverseAsOfJoinResult(JoinMatch.of(firstColumn, secondColumn),
                        ReverseAsOfJoinRule.GREATER_THAN);
            }
        });
    }

    private static <T, R> void registerEqualFactories(
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
        joinParser.registerFactory(new AbstractExpressionFactory<T>(
                SelectFactoryConstants.START_PTRN + "(" + SelectFactoryConstants.ID_PTRN + ")\\s*==?\\s*("
                        + SelectFactoryConstants.ID_PTRN + ")" + SelectFactoryConstants.END_PTRN) {
            @Override
            public T getExpression(String expression, Matcher matcher, Object... args) {
                final ColumnName firstColumn = ColumnName.of(matcher.group(1));
                final ColumnName secondColumn = ColumnName.of(matcher.group(2));
                return factory.apply(JoinMatch.of(firstColumn, secondColumn), rule);
            }
        });
    }

    private static AsOfJoinResult getAjExpression(String match) {
        return asOfJoinParser.parse(match);
    }

    public static AsOfJoinResult getAjExpressions(String... matches) {
        JoinMatch[] result = new JoinMatch[matches.length];
        for (int ii = 0; ii < matches.length - 1; ++ii) {
            result[ii] = JoinMatch.parse(matches[ii]);
        }

        final AsOfJoinResult finalColumn = getAjExpression(matches[matches.length - 1]);
        result[matches.length - 1] = finalColumn.matches[0];

        return new AsOfJoinResult(result, finalColumn.rule);
    }

    public static AsOfJoinResult getAjExpressions(Collection<String> matches) {
        return getAjExpressions(matches.toArray(new String[0]));
    }

    private static ReverseAsOfJoinResult getRajExpression(String match) {
        return reverseAsOfJoinParser.parse(match);
    }

    public static ReverseAsOfJoinResult getRajExpressions(String... matches) {
        JoinMatch[] result = new JoinMatch[matches.length];
        for (int ii = 0; ii < matches.length - 1; ++ii) {
            result[ii] = JoinMatch.parse(matches[ii]);
        }

        final ReverseAsOfJoinResult finalColumn = getRajExpression(matches[matches.length - 1]);
        result[matches.length - 1] = finalColumn.matches[0];

        return new ReverseAsOfJoinResult(result, finalColumn.rule);
    }

    public static ReverseAsOfJoinResult getRajExpressions(Collection<String> matches) {
        return getRajExpressions(matches.toArray(new String[0]));
    }
}
