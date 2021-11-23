package io.deephaven.engine.table.impl.lang;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParseStart;
import com.github.javaparser.Providers;
import com.github.javaparser.ast.expr.Expression;

import java.io.IOException;
import java.io.StringReader;

/**
 * Helpful class which parses expressions and performs extra "this is exactly one expression" validation
 */
public class JavaExpressionParser {

    public static Expression parseExpression(String expression) {
        StringReader sr = new StringReader(expression);
        ParseResult<Expression> result = new JavaParser().parse(ParseStart.EXPRESSION, Providers.provider(sr));
        if (!result.isSuccessful()) {
            throw new IllegalArgumentException(
                    "Invalid expression " + expression + ": " + result.getProblems().toString());
        }

        Expression expr = result.getResult().orElse(null);

        // let's be certain the string reader was drained; or else we might not have parsed what we thought we did
        try {
            int test;
            while ((test = sr.read()) != -1) {
                if (!Character.isWhitespace(test)) {
                    throw new IllegalArgumentException(
                            "Invalid expression " + expression + " was already terminated after " + expr);
                }
            }
            sr.close();
        } catch (IOException ignored) {
        }
        return expr;
    }
}
