/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import io.deephaven.base.Pair;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.AbstractExpressionFactory;
import io.deephaven.db.tables.utils.ExpressionParser;

import java.util.Collection;
import java.util.regex.Matcher;

import static io.deephaven.db.tables.select.SelectFactoryConstants.*;

/**
 * MatchPair Factory that accepts final value of either =, &lt;=, or &lt;, &gt; &gt;= and returns a Pair&lt;MatchPair,
 * Table.AsOfMatchRule&gt;.
 */
public class AjMatchPairFactory {
    private enum AsOfMatchRule {
        Equal, Less_Than, Less_Than_Equal, Greater_Than, Greater_Than_Equal;

        Table.AsOfMatchRule toTableMatchRule(boolean reverse) {
            switch (this) {
                case Equal:
                    return reverse ? Table.AsOfMatchRule.GREATER_THAN_EQUAL : Table.AsOfMatchRule.LESS_THAN_EQUAL;
                case Less_Than:
                    return Table.AsOfMatchRule.LESS_THAN;
                case Less_Than_Equal:
                    return Table.AsOfMatchRule.LESS_THAN_EQUAL;
                case Greater_Than:
                    return Table.AsOfMatchRule.GREATER_THAN;
                case Greater_Than_Equal:
                    return Table.AsOfMatchRule.GREATER_THAN_EQUAL;
            }
            throw new IllegalArgumentException();
        }
    }

    private static final ExpressionParser<Pair<MatchPair, AsOfMatchRule>> finalColumnParser = new ExpressionParser<>();
    static {
        finalColumnParser.registerFactory(new AbstractExpressionFactory<Pair<MatchPair, AsOfMatchRule>>(
                START_PTRN + "(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public Pair<MatchPair, AsOfMatchRule> getExpression(String expression, Matcher matcher, Object... args) {
                String columnName = matcher.group(1);
                return new Pair<>(new MatchPair(columnName, columnName), AsOfMatchRule.Equal);
            }
        });
        finalColumnParser.registerFactory(new AbstractExpressionFactory<Pair<MatchPair, AsOfMatchRule>>(
                START_PTRN + "(" + ID_PTRN + ")\\s*==?\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public Pair<MatchPair, AsOfMatchRule> getExpression(String expression, Matcher matcher, Object... args) {
                return new Pair<>(new MatchPair(matcher.group(1), matcher.group(2)), AsOfMatchRule.Equal);
            }
        });
        finalColumnParser.registerFactory(new AbstractExpressionFactory<Pair<MatchPair, AsOfMatchRule>>(
                START_PTRN + "(" + ID_PTRN + ")\\s*<=\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public Pair<MatchPair, AsOfMatchRule> getExpression(String expression, Matcher matcher, Object... args) {
                return new Pair<>(new MatchPair(matcher.group(1), matcher.group(2)), AsOfMatchRule.Less_Than_Equal);
            }
        });
        finalColumnParser.registerFactory(new AbstractExpressionFactory<Pair<MatchPair, AsOfMatchRule>>(
                START_PTRN + "(" + ID_PTRN + ")\\s*<\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public Pair<MatchPair, AsOfMatchRule> getExpression(String expression, Matcher matcher, Object... args) {
                return new Pair<>(new MatchPair(matcher.group(1), matcher.group(2)), AsOfMatchRule.Less_Than);
            }
        });
        finalColumnParser.registerFactory(new AbstractExpressionFactory<Pair<MatchPair, AsOfMatchRule>>(
                START_PTRN + "(" + ID_PTRN + ")\\s*>=\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public Pair<MatchPair, AsOfMatchRule> getExpression(String expression, Matcher matcher, Object... args) {
                return new Pair<>(new MatchPair(matcher.group(1), matcher.group(2)), AsOfMatchRule.Greater_Than_Equal);
            }
        });
        finalColumnParser.registerFactory(new AbstractExpressionFactory<Pair<MatchPair, AsOfMatchRule>>(
                START_PTRN + "(" + ID_PTRN + ")\\s*>\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public Pair<MatchPair, AsOfMatchRule> getExpression(String expression, Matcher matcher, Object... args) {
                return new Pair<>(new MatchPair(matcher.group(1), matcher.group(2)), AsOfMatchRule.Greater_Than);
            }
        });
    }

    public static Pair<MatchPair, Table.AsOfMatchRule> getExpression(boolean reverse, String match) {
        Pair<MatchPair, AsOfMatchRule> parse = finalColumnParser.parse(match);
        return new Pair<>(parse.first, parse.second.toTableMatchRule(reverse));
    }

    @SuppressWarnings("WeakerAccess")
    public static Pair<MatchPair[], Table.AsOfMatchRule> getExpressions(boolean reverse, String... matches) {
        MatchPair[] result = new MatchPair[matches.length];
        for (int ii = 0; ii < matches.length - 1; ++ii) {
            result[ii] = MatchPairFactory.getExpression(matches[ii]);
        }

        Pair<MatchPair, Table.AsOfMatchRule> finalColumn = getExpression(reverse, matches[matches.length - 1]);
        result[matches.length - 1] = finalColumn.first;

        return new Pair<>(result, finalColumn.second);
    }

    public static Pair<MatchPair[], Table.AsOfMatchRule> getExpressions(boolean reverse, Collection<String> matches) {
        return getExpressions(reverse, matches.toArray(new String[matches.size()]));
    }
}
