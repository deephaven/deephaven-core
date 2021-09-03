/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.utils;

import java.util.regex.Matcher;

public interface ExpressionFactory<TYPE> {
    TYPE getExpression(String expression, Matcher matcher, Object... args);

    String getPattern();
}
