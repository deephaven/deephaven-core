//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link TableAdapter}, primarily to verify completeness.
 */
public class TestTableAdapter {

    private static final class TableAdapterImpl implements TableAdapter {
    }

    @Test
    public void verifyInstantiation() {
        final Table SUT = new TableAdapterImpl();
        try {
            SUT.getDefinition();
            fail("Expected exception");
        } catch (UnsupportedOperationException expected) {
        }
    }
}
