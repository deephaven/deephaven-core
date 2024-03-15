//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.clientsupport.gotorow;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.util.TableTools.intCol;


public class SeekRowTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void singleRow() {
        Table t = TableTools.newTable(intCol("num", 1)).sort("num");
        Assert.assertEquals(0, (long) new SeekRow(
                0, "num", 1, false, false, true).seek(t));
        Assert.assertEquals(0, (long) new SeekRow(
                0, "num", 1, false, false, false).seek(t));
    }

    @Test
    public void ascendingForward() {
        Table t = createSortedTable(true);
        Assert.assertEquals(3, (long) new SeekRow(
                0, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                1, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                2, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(4, (long) new SeekRow(
                3, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                4, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                5, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                6, "num", 3, false, false, false).seek(t));
    }

    @Test
    public void ascendingBackward() {
        Table t = createSortedTable(true);
        Assert.assertEquals(4, (long) new SeekRow(
                0, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(4, (long) new SeekRow(
                1, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(4, (long) new SeekRow(
                2, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(4, (long) new SeekRow(
                3, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                4, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(4, (long) new SeekRow(
                5, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(4, (long) new SeekRow(
                6, "num", 3, false, false, true).seek(t));
    }

    @Test
    public void descendingForward() {
        Table t = createSortedTable(false);
        Assert.assertEquals(2, (long) new SeekRow(
                0, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(2, (long) new SeekRow(
                1, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                2, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(2, (long) new SeekRow(
                3, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(2, (long) new SeekRow(
                4, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(2, (long) new SeekRow(
                5, "num", 3, false, false, false).seek(t));
        Assert.assertEquals(2, (long) new SeekRow(
                6, "num", 3, false, false, false).seek(t));
    }

    @Test
    public void descendingBackward() {
        Table t = createSortedTable(false);
        Assert.assertEquals(3, (long) new SeekRow(
                0, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                1, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                2, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(2, (long) new SeekRow(
                3, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                4, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                5, "num", 3, false, false, true).seek(t));
        Assert.assertEquals(3, (long) new SeekRow(
                6, "num", 3, false, false, true).seek(t));
    }

    private Table createSortedTable(boolean ascending) {
        Table t = TableTools.newTable(intCol("num", 1, 1, 2, 3, 3, 4, 4)).sort("num");
        if (ascending) {
            return t.sort("num");
        } else {
            return t.sortDescending("n");
        }
    }
}
