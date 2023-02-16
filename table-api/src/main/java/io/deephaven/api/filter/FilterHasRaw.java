/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.value.Value;

public class FilterHasRaw implements Filter.Visitor, Expression.Visitor, Value.Visitor {

    public static boolean of(Filter filter) {
        return filter.walk(new FilterHasRaw()).out();
    }

    public static boolean of(Expression expression) {
        return expression.walk(new FilterHasRaw()).out();
    }

    public Boolean hasRaw;

    private FilterHasRaw() {}

    public boolean out() {
        return hasRaw;
    }

    @Override
    public void visit(FilterIsNull isNull) {
        hasRaw = false;
    }

    @Override
    public void visit(FilterIsNotNull isNotNull) {
        hasRaw = false;
    }

    @Override
    public void visit(FilterCondition condition) {
        hasRaw = of(condition.lhs()) || of(condition.rhs());
    }

    @Override
    public void visit(FilterNot not) {
        not.filter().walk(this);
    }

    @Override
    public void visit(FilterOr ors) {
        hasRaw = ors.filters().stream().anyMatch(FilterHasRaw::of);
    }

    @Override
    public void visit(FilterAnd ands) {
        hasRaw = ands.filters().stream().anyMatch(FilterHasRaw::of);
    }

    @Override
    public void visit(RawString rawString) {
        hasRaw = true;
    }

    @Override
    public void visit(Value value) {
        value.walk((Value.Visitor) this);
    }

    @Override
    public void visit(ColumnName x) {
        hasRaw = false;
    }

    @Override
    public void visit(long x) {
        hasRaw = false;
    }
}
