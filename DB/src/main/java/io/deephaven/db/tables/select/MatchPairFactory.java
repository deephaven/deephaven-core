/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import io.deephaven.db.tables.utils.ExpressionParser;
import io.deephaven.db.tables.utils.AbstractExpressionFactory;

import java.util.Collection;
import java.util.regex.Matcher;

import static io.deephaven.db.tables.select.SelectFactoryConstants.*;

/**
 * Parses strings of the form "Column" or "Column1=Column2" into a MatchPair (or array of them).
 */
public class MatchPairFactory {
    private static final ExpressionParser<MatchPair> parser = new ExpressionParser<>();
    static {
        parser.registerFactory(new AbstractExpressionFactory<MatchPair>(START_PTRN + "(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public MatchPair getExpression(String expression, Matcher matcher, Object... args) {
                String columnName = matcher.group(1);
                return new MatchPair(columnName, columnName);
            }
        });
        parser.registerFactory(new AbstractExpressionFactory<MatchPair>(
                START_PTRN + "(" + ID_PTRN + ")\\s*==?\\s*(" + ID_PTRN + ")" + END_PTRN) {
            @Override
            public MatchPair getExpression(String expression, Matcher matcher, Object... args) {
                return new MatchPair(matcher.group(1), matcher.group(2));
            }
        });
    }

    public static MatchPair getExpression(String match) {
        return parser.parse(match);
    }

    public static MatchPair[] getExpressions(String... matches) {
        MatchPair[] result = new MatchPair[matches.length];
        for (int ii = 0; ii < matches.length; ++ii) {
            result[ii] = getExpression(matches[ii]);
        }
        return result;
    }

    public static MatchPair[] getExpressions(Collection<String> matches) {
        return matches.stream().map(MatchPairFactory::getExpression).toArray(MatchPair[]::new);
    }
}
