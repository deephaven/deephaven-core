//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.ConcurrencyControl;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.literal.Literal;
import io.deephaven.api.literal.LiteralFilter;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Represents an evaluate-able filter.
 *
 * @see io.deephaven.api.TableOperations#where(Filter)
 * @see FilterIsNull
 * @see FilterComparison
 * @see FilterIn
 * @see FilterNot
 * @see FilterOr
 * @see FilterAnd
 * @see FilterPattern
 * @see ColumnName
 * @see Function
 * @see Method
 * @see LiteralFilter
 * @see RawString
 */
public interface Filter extends Expression, ConcurrencyControl<Filter> {

    static Collection<? extends Filter> from(String... expressions) {
        return from(Arrays.asList(expressions));
    }

    static Collection<? extends Filter> from(Collection<String> expressions) {
        return expressions.stream().map(RawString::of).collect(Collectors.toList());
    }

    /**
     * Creates an always-true-filter.
     *
     * <p>
     * Equivalent to {@code Literal.of(true)}.
     *
     * @return the always-true-filter
     */
    static LiteralFilter ofTrue() {
        return Literal.of(true);
    }

    /**
     * Creates an always-false-filter.
     *
     * <p>
     * Equivalent to {@code Literal.of(false)}.
     *
     * @return the always-false-filter
     */
    static LiteralFilter ofFalse() {
        return Literal.of(false);
    }

    /**
     * Creates an is-null-filter.
     *
     * @param expression the expression
     * @return the is-null-filter
     */
    static FilterIsNull isNull(Expression expression) {
        return FilterIsNull.of(expression);
    }

    /**
     * Creates an is-not-null-filter.
     *
     * <p>
     * Equivalent to {@code not(isNull(expression))}.
     *
     * @param expression the expression
     * @return the is-not-null-filter
     */
    static FilterNot<FilterIsNull> isNotNull(Expression expression) {
        return not(isNull(expression));
    }

    /**
     * Creates an is-true-filter.
     *
     * <p>
     * Equivalent to {@code FilterComparison.eq(expression, ofTrue())}.
     *
     * @param expression the expression
     * @return the equals-true-filter
     */
    static FilterComparison isTrue(Expression expression) {
        return FilterComparison.eq(expression, ofTrue());
    }

    /**
     * Creates an is-false-filter.
     *
     * <p>
     * Equivalent to {@code FilterComparison.eq(expression, ofFalse())}.
     *
     * @param expression the expression
     * @return @return the equals-false-filter
     */
    static FilterComparison isFalse(Expression expression) {
        return FilterComparison.eq(expression, ofFalse());
    }

    /**
     * Creates a {@link FilterNot not-filter} from {@code filter}. Callers should typically prefer
     * {@link Filter#invert()}, unless the "not" context needs to be preserved.
     *
     * @param filter the filter
     * @return the not-filter
     * @param <F> the type of filter
     */
    static <F extends Filter> FilterNot<F> not(F filter) {
        return FilterNot.of(filter);
    }

    /**
     * Creates a filter that evaluates to {@code true} when any of {@code filters} evaluates to {@code true}, and
     * {@code false} when none of the {@code filters} evaluates to {@code true}. This implies that {@link #ofFalse()} is
     * returned when {@code filters} is empty.
     *
     * @param filters the filters
     * @return the filter
     */
    static Filter or(Filter... filters) {
        return or(Arrays.asList(filters));
    }

    /**
     * Creates a filter that evaluates to {@code true} when any of {@code filters} evaluates to {@code true}, and
     * {@code false} when none of the {@code filters} evaluates to {@code true}. This implies that {@link #ofFalse()} is
     * returned when {@code filters} is empty.
     *
     * @param filters the filters
     * @return the filter
     */
    static Filter or(Collection<? extends Filter> filters) {
        if (filters.isEmpty()) {
            return ofFalse();
        }
        if (filters.size() == 1) {
            return filters.iterator().next();
        }
        return FilterOr.of(filters);
    }

    /**
     * Creates a filter that evaluates to {@code true} when all of the {@code filters} evaluate to {@code true}, and
     * {@code false} when any of the {@code filters} evaluates to {@code false}. This implies that {@link #ofTrue()} is
     * returned when {@code filters} is empty.
     *
     * @param filters the filters
     * @return the filter
     */
    static Filter and(Filter... filters) {
        return and(Arrays.asList(filters));
    }

    /**
     * Creates a filter that evaluates to {@code true} when all of the {@code filters} evaluate to {@code true}, and
     * {@code false} when any of the {@code filters} evaluates to {@code false}. This implies that {@link #ofTrue()} is
     * returned when {@code filters} is empty.
     *
     * @param filters the filters
     * @return the filter
     */
    static Filter and(Collection<? extends Filter> filters) {
        if (filters.isEmpty()) {
            return ofTrue();
        }
        if (filters.size() == 1) {
            return filters.iterator().next();
        }
        return FilterAnd.of(filters);
    }

    /**
     * Wraps the given filter with a FilterSerial to enforce serial execution.
     * <p>
     *
     * @param filter the filter to wrap
     * @return a FilterSerial instance wrapping the provided filter
     */
    static Filter serial(Filter filter) {
        return FilterSerial.of(filter);
    }

    /**
     * Wraps the given filter with a FilterBarrier to declare one or more barriers that other filters can respect.
     *
     * @param filter the filter to wrap
     * @param barriers the barrier objects being declared
     * @return a FilterBarrier instance wrapping the provided filter
     */
    static FilterBarrier barrier(Filter filter, Object... barriers) {
        return FilterBarrier.of(filter, barriers);
    }

    /**
     * Wraps the given filter with a FilterBarrier to declare a barrier that other filters can respect.
     *
     * @param filter the filter to wrap
     * @param barriers the barrier objects that need to be respected
     * @return a FilterBarrier instance wrapping the provided filter
     */
    static FilterRespectsBarrier respectsBarrier(Filter filter, Object... barriers) {
        return FilterRespectsBarrier.of(filter, barriers);
    }

    /**
     * Performs a non-recursive "and-extraction" against {@code filter}. If {@code filter} is a {@link FilterAnd},
     * {@link FilterAnd#filters()} will be returned. If {@code filter} is {@link Filter#ofTrue()}, an empty list will be
     * returned. Otherwise, a singleton list of {@code filter} will be returned.
     *
     * @param filter the filter
     * @return the and-extracted filter
     */
    static Collection<Filter> extractAnds(Filter filter) {
        return ExtractAnds.of(filter);
    }

    /**
     * The logical inversion of {@code this}. While logically equivalent to {@code Filter.not(this)}, implementations of
     * this method will return more specifically typed inversions where applicable.
     *
     * @return the inverse filter
     * @see #not(Filter)
     */
    Filter invert();

    @Override
    default Filter withSerial() {
        return serial(this);
    }

    @Override
    default Filter respectsBarriers(Object... barriers) {
        return respectsBarrier(this, barriers);
    }

    @Override
    default Filter withBarriers(Object... barriers) {
        return barrier(this, barriers);
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {

        T visit(FilterIsNull isNull);

        T visit(FilterComparison comparison);

        T visit(FilterIn in);

        T visit(FilterNot<?> not);

        T visit(FilterOr ors);

        T visit(FilterAnd ands);

        T visit(FilterPattern pattern);

        T visit(FilterSerial serial);

        T visit(FilterBarrier barrier);

        T visit(FilterRespectsBarrier respectsBarrier);

        T visit(Function function);

        T visit(Method method);

        T visit(boolean literal);

        T visit(RawString rawString);
    }
}
