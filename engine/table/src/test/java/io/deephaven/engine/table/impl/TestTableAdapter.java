package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import junit.framework.TestCase;
import org.junit.Test;

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
            TestCase.fail("Expected exception");
        } catch (UnsupportedOperationException expected) {
        }
    }
}
