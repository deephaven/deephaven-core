package io.deephaven.api.filter;

import io.deephaven.api.RawString;

public class FilterHasRaw implements Filter.Visitor {

    public static boolean of(Filter filter) {
        return filter.walk(new FilterHasRaw()).out();
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
        hasRaw = false;
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
}
