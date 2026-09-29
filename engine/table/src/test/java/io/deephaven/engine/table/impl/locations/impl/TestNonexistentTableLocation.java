//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link NonexistentTableLocation}.
 */
public class TestNonexistentTableLocation {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static TableLocation nonexistentLocation() {
        return new NonexistentTableLocation(
                StandaloneTableKey.getInstance(),
                StandaloneTableLocationKey.getInstance());
    }

    @Test
    public void testHasNoDataIndex() {
        assertFalse(nonexistentLocation().hasDataIndex("Sym"));
    }

    /**
     * {@link TableLocation#getDataIndex} returns {@code null} when the location has no index, so a caller need not ask
     * {@link TableLocation#hasDataIndex} first.
     */
    @Test
    public void testGetDataIndexIsNull() {
        assertNull(nonexistentLocation().getDataIndex("Sym"));
    }
}
