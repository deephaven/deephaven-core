package com.github.javaparser;

import com.github.javaparser.ast.expr.Expression;

import java.io.IOException;
import java.io.StringReader;

/**
 * Helpful class which parses expressions and performs extra "this is exactly one expression" validation
 */
public class ExpressionParser {
    public static Expression parseExpression(String expression) throws ParseException, IOException {
        // nothing static here, so no synchronization needed (javaparser used to create a static-only parser instance)
        StringReader sr = new StringReader(expression);
        ParseResult<Expression> result = new JavaParser().parse(ParseStart.EXPRESSION, Providers.provider(sr));
        if (!result.isSuccessful()) {
            throw new IllegalArgumentException(
                    "Invalid expression " + expression + ": " + result.getProblems().toString());
        }
        // a successful parse does not always mean there is a result.
        Expression expr = result.getResult().orElse(null);

        // unlikely for there to be tokens left w/out parser.token.next already being non-null (we would already have
        // thrown...)
        int test;
        try {
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
