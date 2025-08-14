//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.RawString;
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

import java.util.List;

enum FilterToListImpl implements Filter.Visitor<List<Filter>> {
    INSTANCE;

    public static List<Filter> of(Filter filter) {
        // This will return
        // 1. an empty list if filter == Filter.ofTrue()
        // 2. io.deephaven.api.filter.FilterAnd#filters if filter is FilterAnd
        // 3. [ filter ] otherwise
        return filter instanceof WhereFilter ? List.of(filter) : filter.walk(INSTANCE);
    }

    @Override
    public List<Filter> visit(FilterAnd ands) {
        return ands.filters();
    }

    @Override
    public List<Filter> visit(boolean literal) {
        return literal ? List.of() : List.of(Filter.ofFalse());
    }

    @Override
    public List<Filter> visit(FilterIsNull isNull) {
        return List.of(isNull);
    }

    @Override
    public List<Filter> visit(FilterComparison comparison) {
        return List.of(comparison);
    }

    @Override
    public List<Filter> visit(FilterIn in) {
        return List.of(in);
    }

    @Override
    public List<Filter> visit(FilterNot<?> not) {
        return List.of(not);
    }

    @Override
    public List<Filter> visit(FilterOr ors) {
        return List.of(ors);
    }

    @Override
    public List<Filter> visit(FilterPattern pattern) {
        return List.of(pattern);
    }

    @Override
    public List<Filter> visit(FilterSerial serial) {
        return List.of(serial);
    }

    @Override
    public List<Filter> visit(FilterBarrier barrier) {
        return List.of(barrier);
    }

    @Override
    public List<Filter> visit(FilterRespectsBarrier respectsBarrier) {
        return List.of(respectsBarrier);
    }

    @Override
    public List<Filter> visit(Function function) {
        return List.of(function);
    }

    @Override
    public List<Filter> visit(Method method) {
        return List.of(method);
    }

    @Override
    public List<Filter> visit(RawString rawString) {
        return List.of(rawString);
    }
}
