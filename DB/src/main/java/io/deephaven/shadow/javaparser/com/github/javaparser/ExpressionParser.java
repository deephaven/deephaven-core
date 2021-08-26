package com.github.javaparser;

import com.github.javaparser.ast.expr.Expression;

import java.io.IOException;
import java.io.StringReader;

/**
 * Helpful class which parses expressions and performs extra "this is exactly one expression"
 * validation
 */
public class ExpressionParser {
    public static Expression parseExpression(String expression) throws ParseException, IOException {
        // nothing static here, so no synchronization needed (javaparser used to create a
        // static-only parser instance)
        StringReader sr = new StringReader(expression);
        final ASTParser parser = new ASTParser(sr);
        final Expression expr = parser.Expression();

        Token token = parser.token;
        while (token.next != null) {
            if (token.kind == ASTParserConstants.EOF) {
                return expr;
            }
            if (token.next.image.trim().isEmpty()) {
                token = token.next;
                continue;
            }
            // there was a leftover token; javacc recognizes the token, but there's no valid ast
            // that it can construct.
            // we don't notice that, however, since we throw it way.
            throw new IllegalArgumentException(
                "Invalid expression " + expression + " was already terminated after " + expr);
        }
        // unlikely for there to be tokens left w/out parser.token.next already being non-null (we
        // would already have thrown...)
        int test;
        try {
            while ((test = sr.read()) != -1) {
                if (!Character.isWhitespace(test)) {
                    throw new IllegalArgumentException("Invalid expression " + expression
                        + " was already terminated after " + expr);
                }
            }
            sr.close();
        } catch (IOException ignored) {
        }
        return expr;
    }
}
