package io.deephaven.sql;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.Filter.Visitor;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIn;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.literal.Literal;

import java.util.Collection;
import java.util.Collections;

public enum ExtractAnds implements Visitor<Collection<Filter>> {
    INSTANCE;

    public static Collection<Filter> of(Filter filter) {
        return filter.walk(INSTANCE);
    }

    @Override
    public Collection<Filter> visit(FilterIsNull isNull) {
        return Collections.singleton(isNull);
    }

    @Override
    public Collection<Filter> visit(FilterComparison comparison) {
        return Collections.singleton(comparison);
    }

    @Override
    public Collection<Filter> visit(FilterIn in) {
        return Collections.singleton(in);
    }

    @Override
    public Collection<Filter> visit(FilterNot<?> not) {
        return Collections.singleton(not);
    }

    @Override
    public Collection<Filter> visit(FilterOr ors) {
        return Collections.singleton(ors);
    }

    @Override
    public Collection<Filter> visit(FilterAnd ands) {
        return ands.filters();
    }

    @Override
    public Collection<Filter> visit(FilterPattern pattern) {
        return Collections.singleton(pattern);
    }

    @Override
    public Collection<Filter> visit(Function function) {
        return Collections.singleton(function);
    }

    @Override
    public Collection<Filter> visit(Method method) {
        return Collections.singleton(method);
    }

    @Override
    public Collection<Filter> visit(boolean literal) {
        if (literal) {
            return Collections.emptyList();
        } else {
            return Collections.singleton(Literal.of(false));
        }
    }

    @Override
    public Collection<Filter> visit(RawString rawString) {
        return Collections.singleton(rawString);
    }
}
