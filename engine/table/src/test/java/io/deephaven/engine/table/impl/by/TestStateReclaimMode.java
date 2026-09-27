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
        assertFalse(StateReclaimMode.releaseBlocks(1, -1, false).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(0.5, -1, false).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(1, 0, false).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(1, 0, true).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(1, 0, true).bulkShift());
        assertFalse(StateReclaimMode.releaseBlocks(1, 0, false).bulkShift());
    }

    @Test
    public void testConfiguredModesMayDegrade() {
        assertTrue(StateReclaimMode.configured().isConfigured() || !StateReclaimMode.configured().reclaims());
        assertFalse(StateReclaimMode.releaseBlocks(1, 0, true).isConfigured());
        assertFalse(StateReclaimMode.releaseBlocks(1, -1, false).isConfigured());
        assertFalse(StateReclaimMode.none().isConfigured());
    }

    @Test
    public void testRejectsNaNFractions() {
        assertThrows(IllegalArgumentException.class, () -> StateReclaimMode.releaseBlocks(Double.NaN, -1, false));
        assertThrows(IllegalArgumentException.class, () -> StateReclaimMode.releaseBlocks(1, Double.NaN, false));
    }
}
