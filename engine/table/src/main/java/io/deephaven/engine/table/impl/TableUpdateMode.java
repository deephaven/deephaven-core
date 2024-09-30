//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

/**
 * Records the guarantees that the table offers regarding addition and removal of data elements. This can apply to
 * {@link SourceTable sourc table} location additions and removals or row additions and removals within a location.
 */
public enum TableUpdateMode {
    STATIC, APPEND_ONLY, ADD_ONLY, ADD_REMOVE;

    public boolean addAllowed() {
        switch (this) {
            case APPEND_ONLY:
            case ADD_ONLY:
            case ADD_REMOVE:
                return true;

            default:
                return false;
        }
    }

    public boolean removeAllowed() {
        switch (this) {
            case ADD_REMOVE:
                return true;

            default:
                return false;
        }
    }
}
