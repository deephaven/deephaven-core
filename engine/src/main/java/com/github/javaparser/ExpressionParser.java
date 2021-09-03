package com.github.javaparser;

import com.github.javaparser.ast.expr.Expression;

import java.io.IOException;
import java.io.StringReader;

/**
 * Helpful class which parses expressions and performs extra "this is exactly one expression" validation
 */
public class ExpressionParser {
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
