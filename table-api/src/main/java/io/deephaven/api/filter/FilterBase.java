/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

public abstract class FilterBase implements Filter {

    @Override
    public final FilterNot not() {
        return FilterNot.of(this);
    }
}
