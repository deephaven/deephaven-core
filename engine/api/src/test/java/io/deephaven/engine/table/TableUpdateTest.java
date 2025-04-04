//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import org.junit.Assert;
import org.junit.Test;

public class TableUpdateTest {
    @Test
    public void testValidate() {
        final RowSet added = RowSetFactory.fromKeys(0);
        final RowSet removed = RowSetFactory.fromKeys(1);
        final RowSet modified = RowSetFactory.fromKeys(2);
        final TableUpdate tableUpdate =
                new TableUpdateImpl(added, removed, modified, RowSetShiftData.EMPTY, ModifiedColumnSet.ALL);
        tableUpdate.validate();
        added.close();
        final AssertionFailure af1 = Assert.assertThrows(AssertionFailure.class, tableUpdate::validate);
        Assert.assertEquals(
                "Assertion failed: asserted Invalid RowSet in TableUpdateImpl, NullPointerException when calling isEmpty is never executed.",
                af1.getMessage());

        final RowSet added2 = RowSetFactory.fromKeys(3);

        final TableUpdate tableUpdate2 =
                new TableUpdateImpl(added2, removed, modified, RowSetShiftData.EMPTY, ModifiedColumnSet.ALL);
        tableUpdate2.validate();
        tableUpdate2.release();
        final AssertionFailure af2 = Assert.assertThrows(AssertionFailure.class, tableUpdate2::validate);
        Assert.assertEquals("Assertion failed: asserted added != null, instead added == null.", af2.getMessage());
    }
}
