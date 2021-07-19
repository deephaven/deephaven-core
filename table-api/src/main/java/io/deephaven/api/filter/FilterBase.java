package io.deephaven.api.filter;

public abstract class FilterBase implements Filter {

    @Override
    public final FilterNot not() {
        return FilterNot.of(this);
    }
}
