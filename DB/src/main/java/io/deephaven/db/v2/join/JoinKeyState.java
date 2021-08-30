/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.join;

import io.deephaven.db.v2.utils.Index;

interface JoinKeyState {
    void addLeftIndices(Index leftIndices);

    void removeLeftIndices(Index leftIndices);

    void addRightIndices(Index rightIndices);

    void removeRightIndices(Index rightIndices);

    void modifyByRightIndices(Index rightIndex);// Informs the state the right index at that
                                                // position was modified

    /**
     * After the right side has been changed (all additions, modifications, removals, etc.) have
     * been completed; each state is visited calling propagateRightUpdates to update its
     * RedirectionIndex and the list of left indicesthat have been modified by right changes.
     */
    void propagateRightUpdates();

    boolean isActive();

    void setActive();

    String dumpString();

    /**
     * Get the key for this join state, for use within the statesByKey KeyedObjectHashMap.
     */
    Object getKey();

    /**
     * Intrusive set for touchedStates or statesTouchedByRight.
     *
     * The sets can swap back and forth; so rather than having to remove things from one set and
     * enter them into another; we swap which of the two intrusive references we use.
     */
    int getSlot1();

    void setSlot1(int slot);

    /**
     * Second intrusive list for touchedStates or statesTouchedByRight.
     */
    int getSlot2();

    void setSlot2(int slot);
}
