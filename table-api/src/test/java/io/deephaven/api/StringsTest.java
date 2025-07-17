//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.ExpressionTest;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterBarrier;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterRespectsBarrier;
import io.deephaven.api.filter.FilterSerial;
import io.deephaven.api.filter.FilterTest;
import io.deephaven.api.literal.Literal;
import io.deephaven.api.literal.LiteralTest;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;

public class StringsTest {

    private static void ensureExplicitStringOf(Class<?> clazz) {
        final Lookup lookup = MethodHandles.lookup();
        final MethodType type = MethodType.methodType(String.class, clazz);
        try {
            lookup.findStatic(Strings.class, "of", type);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void expressionAndSubtypesHaveStringOf() {
        ensureExplicitStringOf(Expression.class);
        ExpressionTest.visitAll(EnsureExplicitStringOf.INSTANCE);
    }

    @Test
    void literalAndSubtypesHaveStringOf() {
        ensureExplicitStringOf(Literal.class);
        LiteralTest.visitAll(EnsureExplicitStringOf.INSTANCE);
    }

    @Test
    void filterAndSubtypesHaveStringOf() {
        ensureExplicitStringOf(Filter.class);
        FilterTest.visitAll(EnsureExplicitStringOf.INSTANCE);
    }

    public enum EnsureExplicitStringOf
            implements Expression.Visitor<Void>, Literal.Visitor<Void>, Filter.Visitor<Void> {
        INSTANCE;

        @Override
        public Void visit(Literal literal) {
            ensureExplicitStringOf(Literal.class);
            return null;
        }

        @Override
        public Void visit(FilterIsNull isNull) {
            ensureExplicitStringOf(FilterIsNull.class);
            return null;
        }

        @Override
        public Void visit(FilterComparison comparison) {
            ensureExplicitStringOf(FilterComparison.class);
            return null;
        }

        @Override
        public Void visit(FilterIn in) {
            ensureExplicitStringOf(FilterIn.class);
            return null;
        }

        @Override
        public Void visit(FilterNot<?> not) {
            ensureExplicitStringOf(FilterNot.class);
            return null;
        }

        @Override
        public Void visit(FilterOr ors) {
            ensureExplicitStringOf(FilterOr.class);
            return null;
        }

        @Override
        public Void visit(FilterAnd ands) {
            ensureExplicitStringOf(FilterAnd.class);
            return null;
        }

        @Override
        public Void visit(FilterPattern pattern) {
            ensureExplicitStringOf(FilterPattern.class);
            return null;
        }

        @Override
        public Void visit(FilterSerial serial) {
            ensureExplicitStringOf(FilterSerial.class);
            return null;
        }

        @Override
        public Void visit(FilterBarrier barrier) {
            ensureExplicitStringOf(FilterBarrier.class);
            return null;
        }

        @Override
        public Void visit(FilterRespectsBarrier respectsBarrier) {
            ensureExplicitStringOf(FilterRespectsBarrier.class);
            return null;
        }


        @Override
        public Void visit(ColumnName columnName) {
            ensureExplicitStringOf(ColumnName.class);
            return null;
        }

        @Override
        public Void visit(Filter filter) {
            ensureExplicitStringOf(Filter.class);
            return null;
        }

        @Override
        public Void visit(Function function) {
            ensureExplicitStringOf(Function.class);
            return null;
        }

        @Override
        public Void visit(Method method) {
            ensureExplicitStringOf(Method.class);
            return null;
        }

        @Override
        public Void visit(RawString rawString) {
            ensureExplicitStringOf(RawString.class);
            return null;
        }


        @Override
        public Void visit(boolean literal) {
            ensureExplicitStringOf(boolean.class);
            return null;
        }

        @Override
        public Void visit(char literal) {
            ensureExplicitStringOf(char.class);
            return null;
        }

        @Override
        public Void visit(byte literal) {
            ensureExplicitStringOf(byte.class);
            return null;
        }

        @Override
        public Void visit(short literal) {
            ensureExplicitStringOf(short.class);
            return null;
        }


        @Override
        public Void visit(int literal) {
            ensureExplicitStringOf(int.class);
            return null;
        }

        @Override
        public Void visit(long literal) {
            ensureExplicitStringOf(long.class);
            return null;
        }

        @Override
        public Void visit(float literal) {
            ensureExplicitStringOf(float.class);
            return null;
        }

        @Override
        public Void visit(double literal) {
            ensureExplicitStringOf(double.class);
            return null;
        }

        @Override
        public Void visit(String literal) {
            ensureExplicitStringOf(String.class);
            return null;
        }
    }
}
