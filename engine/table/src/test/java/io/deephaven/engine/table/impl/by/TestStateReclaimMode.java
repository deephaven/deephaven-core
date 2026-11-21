//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestStateReclaimMode {
    @Test
    public void testMovesStates() {
        assertFalse(StateReclaimMode.none().movesStates());
        assertFalse(StateReclaimMode.releaseBlocks(1).movesStates());
        assertFalse(StateReclaimMode.releaseBlocks(2).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(0.5).movesStates());
        assertTrue(StateReclaimMode.releaseBlocks(0).movesStates());
    }

    @Test
    public void testConfiguredModesMayDegrade() {
        assertTrue(StateReclaimMode.configured().isConfigured() || !StateReclaimMode.configured().reclaims());
        assertFalse(StateReclaimMode.releaseBlocks(0.5).isConfigured());
        assertFalse(StateReclaimMode.releaseBlocks(1).isConfigured());
        assertFalse(StateReclaimMode.none().isConfigured());
    }

    @Test
    public void testClampsFraction() {
        assertEquals(1.0, StateReclaimMode.releaseBlocks(2).collapseFreeFraction(), 0.0);
        assertEquals(0.0, StateReclaimMode.releaseBlocks(-1).collapseFreeFraction(), 0.0);
        assertEquals(0.0, StateReclaimMode.releaseBlocks(Double.NEGATIVE_INFINITY).collapseFreeFraction(), 0.0);
        assertEquals(0.5, StateReclaimMode.releaseBlocks(0.5).collapseFreeFraction(), 0.0);
        assertTrue(StateReclaimMode.releaseBlocks(-1).movesStates());
        assertFalse(StateReclaimMode.releaseBlocks(Double.POSITIVE_INFINITY).movesStates());
    }

    @Test
    public void testRejectsNaNFraction() {
        assertThrows(IllegalArgumentException.class, () -> StateReclaimMode.releaseBlocks(Double.NaN));
    }
}
