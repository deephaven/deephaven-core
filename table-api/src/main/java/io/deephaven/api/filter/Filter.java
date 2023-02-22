/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.value.Literal;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Represents an evaluate-able filter.
 *
 * @see io.deephaven.api.TableOperations#where(Collection)
 */
public interface Filter extends Expression, Serializable {

    static Collection<? extends Filter> from(String... expressions) {
        return from(Arrays.asList(expressions));
    }

    static Collection<? extends Filter> from(Collection<String> expressions) {
        return expressions.stream().map(RawString::of).collect(Collectors.toList());
    }

    static Collection<? extends Filter> from_(String... expressions) {
        // This is for Python to invoke "from" without syntax errors.
        return from(expressions);
    }

    static FilterIsNull isNull(Expression expression) {
        // todo: comparison?
        return FilterIsNull.of(expression);
    }

    static FilterIsNotNull isNotNull(Expression expression) {
        // todo: comparison?
        return FilterIsNotNull.of(expression);
    }

    static Filter isTrue(Expression expression) {
        return FilterComparison.eq(expression, Literal.of(true));
    }

    static Filter isFalse(Expression expression) {
        return FilterComparison.eq(expression, Literal.of(false));
    }

    static FilterNot not(Filter filter) {
        return FilterNot.of(filter);
    }

    FilterNot not();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        // TODO (deephaven-core#829): Add more table api Filter structuring

        // note: isNull is a "special" case, as the caller doesn't technically need to know the return type of the
        // expression (even though they should technically know, and the engine will implicitly choose the appropriate
        // call).
        //
        // The same can't be said about boolean; if you want to check whether an expression is true, that will be
        // represented with a filter comparison against a literal boolean

        void visit(FilterIsNull isNull);

        void visit(FilterIsNotNull isNotNull);

        void visit(FilterComparison comparison);

        void visit(FilterNot not);

        void visit(FilterOr ors);

        void visit(FilterAnd ands);

        void visit(RawString rawString);
    }
}
