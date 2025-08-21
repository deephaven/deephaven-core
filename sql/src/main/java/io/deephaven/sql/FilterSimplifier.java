//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.Filter.Visitor;
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
import io.deephaven.api.literal.Literal;

import java.util.ArrayList;
import java.util.List;

enum FilterSimplifier implements Visitor<Filter> {
    INSTANCE;

    public static Filter of(Filter filter) {
        return filter.walk(FilterSimplifier.INSTANCE);
    }

    @Override
    public Filter visit(FilterIsNull isNull) {
        return isNull;
    }

    @Override
    public Filter visit(FilterComparison comparison) {
        return comparison.maybeTranspose();
    }

    @Override
    public Filter visit(FilterNot<?> not) {
        return not.filter().walk(INSTANCE).invert();
    }

    @Override
    public Filter visit(FilterOr ors) {
        final List<Filter> filters = new ArrayList<>(ors.filters().size());
        for (Filter filter : ors.filters()) {
            final Filter simplified = of(filter);
            if (simplified.equals(Literal.of(true))) {
                return Literal.of(true);
            }
            if (simplified.equals(Literal.of(false))) {
                continue;
            }
            filters.add(simplified);
        }
        return Filter.or(filters);
    }

    @Override
    public Filter visit(FilterAnd ands) {
        final List<Filter> filters = new ArrayList<>(ands.filters().size());
        for (Filter filter : ands.filters()) {
            final Filter simplified = of(filter);
            if (simplified.equals(Literal.of(false))) {
                return Literal.of(false);
            }
            if (simplified.equals(Literal.of(true))) {
                continue;
            }
            filters.add(simplified);
        }
        return Filter.and(filters);
    }

    @Override
    public Filter visit(FilterPattern pattern) {
        return pattern;
    }

    @Override
    public Filter visit(FilterSerial serial) {
        return of(serial.filter()).withSerial();
    }

    @Override
    public Filter visit(FilterBarrier barrier) {
        return barrier.filter().walk(this).withBarriers(barrier.barriers());
    }

    @Override
    public Filter visit(FilterRespectsBarrier respectsBarrier) {
        return respectsBarrier.filter().walk(this).respectsBarriers(respectsBarrier.respectedBarriers());
    }

    @Override
    public Filter visit(FilterIn in) {
        return in;
    }

    @Override
    public Filter visit(Function function) {
        return function;
    }

    @Override
    public Filter visit(Method method) {
        return method;
    }

    @Override
    public Filter visit(boolean literal) {
        return Literal.of(literal);
    }

    @Override
    public Filter visit(RawString rawString) {
        return rawString;
    }
}
