/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.db.exceptions.ExpressionException;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * A parser that will try a set of {@link ExpressionFactory}s and attempt to parse the expression
 * until one of them succeeds.
 *
 * @param <TYPE> The expected type of the parsed expression
 */
public class ExpressionParser<TYPE> {
    private Map<Pattern, ExpressionFactory<TYPE>> expressions = new LinkedHashMap<>();

    /**
     * Attempt to process the expression using the {@link #registerFactory(ExpressionFactory)
     * configured} {@link ExpressionFactory factories}
     *
     * @param expression the expression to parse
     * @return The result of the parsing
     *
     * @throws ExpressionException if there is a problem parsing the expression, or no parsers
     *         accepted the expression.
     */
    @NotNull
    public TYPE parse(String expression, Object... args) {
        Throwable creationException = null;
        for (Map.Entry<Pattern, ExpressionFactory<TYPE>> patternExpressionFactoryEntry : expressions
            .entrySet()) {
            Matcher matcher = patternExpressionFactoryEntry.getKey().matcher(expression);
            if (matcher.matches()) {
                try {
                    return patternExpressionFactoryEntry.getValue().getExpression(expression,
                        matcher, args);
                } catch (Throwable t) {
                    if (creationException == null) {
                        creationException = t;
                    }
                }
            }
        }
        if (creationException == null) {
            throw new ExpressionException("Unable to parse expression: \"" + expression + "\"",
                expression);
        } else {
            throw new ExpressionException("Failed to get expression for all matched patterns",
                creationException, expression);
        }
    }

    /**
     * Add an expression factory to the list of possible parsers for an expression.
     *
     * @param expressionFactory the factory
     */
    public void registerFactory(ExpressionFactory<TYPE> expressionFactory) {
        expressions.put(Pattern.compile(expressionFactory.getPattern()), expressionFactory);
    }
}
