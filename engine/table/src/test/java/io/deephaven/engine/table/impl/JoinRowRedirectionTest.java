//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JoinRowRedirectionTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    /**
     * A hash redirection is sized from the left table only as an initial capacity, so a left table of more than
     * Integer.MAX_VALUE rows still gets one.
     */
    @Test
    public void testHashRedirectionForMoreThanIntegerMaxRows() {
        final QueryTable left =
                new QueryTable(RowSetFactory.fromRange(1, 3_000_000_000L).toTracking(), Collections.emptyMap());
        final JoinControl control = new JoinControl() {
            @Override
            RedirectionType getRedirectionType(final Table leftTable) {
                return RedirectionType.Hash;
            }
        };

        final WritableRowRedirection rowRedirection = JoinRowRedirection.makeRowRedirection(control, left);
        assertNotNull(rowRedirection);
        assertEquals(-1, rowRedirection.get(1));
    }
}
