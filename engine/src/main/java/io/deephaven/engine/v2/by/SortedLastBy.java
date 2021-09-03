/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

public final class SortedLastBy extends SortedFirstOrLastByFactoryImpl {
    public SortedLastBy(String... sortColumnNames) {
        super(false, sortColumnNames);
    }
}
