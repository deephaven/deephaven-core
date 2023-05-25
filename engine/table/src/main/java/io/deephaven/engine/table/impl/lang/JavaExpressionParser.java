/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.lang;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParseStart;
import com.github.javaparser.Providers;
import com.github.javaparser.ast.expr.AssignExpr;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.EnclosedExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.LiteralExpr;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;

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

    /**
     * returns true if the expression evaluates to constant value.
     *
     * @param expression the expression to evaluate
     * @return true if the expression evaluates to constant value
     */
    public static boolean isConstantValueExpression(Expression expression) {
        LinkedList<Expression> expressionsQueue = new LinkedList<>();
        expressionsQueue.add(expression);
        while (!expressionsQueue.isEmpty()) {
            Expression currExpr = expressionsQueue.poll();
            if (currExpr instanceof EnclosedExpr) {
                expressionsQueue.add(((EnclosedExpr) currExpr).getInner());
            } else if (currExpr instanceof AssignExpr) {
                expressionsQueue.add(((AssignExpr) currExpr).getValue());
            } else if (currExpr instanceof BinaryExpr) {
                expressionsQueue.add(((BinaryExpr) currExpr).getLeft());
                expressionsQueue.add(((BinaryExpr) currExpr).getRight());
            } else if (!(currExpr instanceof LiteralExpr)) {
                return false;
            }
        }
        return true;
    }
}
