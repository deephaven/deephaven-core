//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.Filter.Visitor;
import io.deephaven.api.literal.Literal;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

enum ExtractRespectedBarriers implements Visitor<Collection<Object>> {
    INSTANCE;

    public static Collection<Object> of(Filter filter) {
        return filter.walk(INSTANCE);
    }

    @Override
    public Collection<Object> visit(FilterIsNull isNull) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterComparison comparison) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterIn in) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterNot<?> not) {
        return not.filter().walk(this);
    }

    @Override
    public Collection<Object> visit(FilterOr ors) {
        return ors.filters().stream()
                .flatMap(filter -> filter.walk(this).stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<Object> visit(FilterAnd ands) {
        return ands.filters().stream()
                .flatMap(filter -> filter.walk(this).stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<Object> visit(FilterPattern pattern) {
        return Collections.emptyList();
    }

    @Override
    public Collection<Object> visit(FilterSerial serial) {
        return serial.filter().walk(this);
    }

    @Override
    public Collection<Object> visit(FilterBarrier barrier) {
        return barrier.filter().walk(this);
    }

    @Override
    public Collection<Object> visit(FilterRespectsBarrier respectsBarrier) {
        return Stream.concat(
                Stream.of(respectsBarrier.respectedBarriers()),
                respectsBarrier.filter().walk(this).stream())
                .collect(Collectors.toSet());
    }

    @Override
    public Collection<Object> visit(Function function) {
        return Collections.singleton(function);
    }

    @Override
    public Collection<Object> visit(Method method) {
        return Collections.singleton(method);
    }

    @Override
    public Collection<Object> visit(boolean literal) {
        if (literal) {
            return Collections.emptyList();
        } else {
            return Collections.singleton(Literal.of(false));
        }
    }

    @Override
    public Collection<Object> visit(RawString rawString) {
        return Collections.singleton(rawString);
    }
}
