//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import org.apache.commons.lang3.mutable.MutableBoolean;

import java.util.stream.Stream;

/**
 * Records the guarantees that the table offers regarding addition and removal of data elements. This can apply to
 * {@link SourceTable source table} location additions and removals or row additions and removals within a location.
 */
public enum TableUpdateMode {
    STATIC, APPEND_ONLY, ADD_ONLY, ADD_REMOVE;

    /**
     * Returns true if addition is allowed.
     */
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

    /**
     * Returns true if removal is allowed.
     */
    public boolean removeAllowed() {
        switch (this) {
            case ADD_REMOVE:
                return true;

            default:
                return false;
        }
    }

    /**
     * Returns the most permissive mode from the given stream of modes. Permissiveness is ranked as follows (from most
     * to least permissive):
     * <ul>
     * <li>{@link #ADD_REMOVE}</li>
     * <li>{@link #ADD_ONLY}</li>
     * <li>{@link #APPEND_ONLY}</li>
     * <li>{@link #STATIC}</li>
     * </ul>
     *
     * @param modes a stream of modes
     * @return the most permissive mode encountered in the stream
     */
    public static TableUpdateMode mostPermissiveMode(Stream<TableUpdateMode> modes) {
        final MutableBoolean anyRemoves = new MutableBoolean(false);
        final MutableBoolean anyAdditions = new MutableBoolean(false);
        final MutableBoolean anyAppends = new MutableBoolean(false);

        modes.forEach(mode -> {
            if (mode.removeAllowed()) {
                anyRemoves.setTrue();
            } else if (mode == TableUpdateMode.ADD_ONLY) {
                anyAdditions.setTrue();
            } else if (mode == TableUpdateMode.APPEND_ONLY) {
                anyAppends.setTrue();
            }
        });
        // @formatter:off
        return
              anyRemoves.booleanValue()   ? TableUpdateMode.ADD_REMOVE
            : anyAdditions.booleanValue() ? TableUpdateMode.ADD_ONLY
            : anyAppends.booleanValue()   ? TableUpdateMode.APPEND_ONLY
            : TableUpdateMode.STATIC;
        // @formatter:on
    }
}
