//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.api.expression.ExpressionParser;
import io.deephaven.api.expression.AbstractExpressionFactory;

import java.util.Collection;
import java.util.regex.Matcher;

import static io.deephaven.api.expression.SelectFactoryConstants.*;

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
