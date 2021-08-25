/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.select;

import io.deephaven.db.tables.utils.AbstractExpressionFactory;
import io.deephaven.db.tables.utils.ExpressionParser;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.stream.Stream;

import static io.deephaven.db.tables.select.SelectFactoryConstants.*;

/**
 * Parses strings of the form "Column1=expression" into a {@link WouldMatchPair} (or array of them).
 */
public class WouldMatchPairFactory {
    private static final ExpressionParser<WouldMatchPair> parser = new ExpressionParser<>();
    static {
        parser.registerFactory(new AbstractExpressionFactory<WouldMatchPair>(
                START_PTRN + "(" + ID_PTRN + ")\\s*=\\s*(" + ANYTHING + ")" + END_PTRN) {
            @Override
            public WouldMatchPair getExpression(String expression, Matcher matcher, Object... args) {
                return new WouldMatchPair(matcher.group(1), matcher.group(2));
            }
        });
    }

    public static WouldMatchPair getExpression(String match) {
        return parser.parse(match);
    }

    public static WouldMatchPair[] getExpressions(String... matches) {
        return getExpressions(Arrays.stream(matches));

    }

    public static WouldMatchPair[] getExpressions(Collection<String> matches) {
        return getExpressions(matches.stream());
    }

    private static WouldMatchPair[] getExpressions(Stream<String> matchesStream) {
        return matchesStream.map(WouldMatchPairFactory::getExpression)
                .toArray(WouldMatchPair[]::new);
    }
}
