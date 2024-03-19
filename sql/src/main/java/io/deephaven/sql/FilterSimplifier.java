//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import io.deephaven.api.RawString;
import io.deephaven.api.expression.Function;
import io.deephaven.api.expression.Method;
import io.deephaven.api.filter.*;
import io.deephaven.api.filter.Filter.Visitor;
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

    @Override
    public Filter visit(StatefulFilter filter) {
        final Filter simplifiedInner = filter.innerFilter().walk(this);
        if (simplifiedInner instanceof StatefulFilter) {
            return simplifiedInner;
        }
        if (simplifiedInner instanceof FilterNot) {
            return FilterNot.of(StatefulFilter.of(((FilterNot<?>) simplifiedInner).filter()));
        } else {
            return StatefulFilter.of(simplifiedInner);
        }
    }
}
