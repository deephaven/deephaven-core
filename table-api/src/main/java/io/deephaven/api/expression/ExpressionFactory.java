/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.expression;

import java.util.regex.Matcher;

public interface ExpressionFactory<TYPE> {
    TYPE getExpression(String expression, Matcher matcher, Object... args);

    String getPattern();
}
