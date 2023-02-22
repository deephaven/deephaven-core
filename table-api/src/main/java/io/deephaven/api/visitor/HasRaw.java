/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.visitor;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.expression.Expression;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.filter.FilterIsNotNull;
import io.deephaven.api.filter.FilterIsNull;
import io.deephaven.api.filter.FilterNot;
import io.deephaven.api.filter.FilterOr;
import io.deephaven.api.value.Value;

public final class HasRaw implements Filter.Visitor, Expression.Visitor, Value.Visitor {

    public static boolean of(Filter filter) {
        final HasRaw visitor = new HasRaw();
        filter.walk((Filter.Visitor) visitor);
        return visitor.out();
    }

    public static boolean of(Expression expression) {
        final HasRaw visitor = new HasRaw();
        expression.walk((Expression.Visitor) visitor);
        return visitor.out();
    }

    public static boolean of(Value value) {
        final HasRaw visitor = new HasRaw();
        value.walk((Value.Visitor) visitor);
        return visitor.out();
    }

    public Boolean hasRaw;

    private HasRaw() {}

    public boolean out() {
        return hasRaw;
    }

    @Override
    public void visit(Value value) {
        value.walk((Value.Visitor) this);
    }

    @Override
    public void visit(Filter filter) {
        filter.walk((Filter.Visitor) this);
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
    public void visit(FilterComparison condition) {
        hasRaw = of(condition.lhs()) || of(condition.rhs());
    }

    @Override
    public void visit(FilterNot not) {
        not.filter().walk((Filter.Visitor) this);
    }

    @Override
    public void visit(FilterOr ors) {
        hasRaw = ors.filters().stream().anyMatch(HasRaw::of);
    }

    @Override
    public void visit(FilterAnd ands) {
        hasRaw = ands.filters().stream().anyMatch(HasRaw::of);
    }

    @Override
    public void visit(RawString rawString) {
        hasRaw = true;
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
