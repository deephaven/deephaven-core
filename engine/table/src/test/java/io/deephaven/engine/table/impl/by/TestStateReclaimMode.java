//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestStateReclaimMode {
    @Test
    public void testMovesStates() {
        assertFalse(StateReclaimMode.none().movesStates());
        assertFalse(StateReclaimMode.releaseBlocks(1, -1).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(0.5, -1).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(1, 0).movesStates());
        assertTrue(StateReclaimMode.credit().movesStates());
        assertTrue(StateReclaimMode.credit().usesCredit());
        assertFalse(StateReclaimMode.releaseBlocks(1, 0).usesCredit());
    }

    @Test
    public void testRejectsNaNFractions() {
        assertThrows(IllegalArgumentException.class, () -> StateReclaimMode.releaseBlocks(Double.NaN, -1));
        assertThrows(IllegalArgumentException.class, () -> StateReclaimMode.releaseBlocks(1, Double.NaN));
    }
}
