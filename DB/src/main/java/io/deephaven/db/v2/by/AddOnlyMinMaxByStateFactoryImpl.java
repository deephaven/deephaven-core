/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

/**
 * If you've got a table that is grow only, this will do a min/max calculation without requiring any state. The
 * limitation is that if you modify or remove a row it will throw an UnsupportedOperationException.
 */
public class AddOnlyMinMaxByStateFactoryImpl extends MinMaxByStateFactoryImpl {
    public AddOnlyMinMaxByStateFactoryImpl(boolean minimum) {
        super(minimum, true);
    }
}
