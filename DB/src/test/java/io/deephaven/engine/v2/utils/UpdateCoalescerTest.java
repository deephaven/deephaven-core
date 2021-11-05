package io.deephaven.engine.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.v2.Listener;
import io.deephaven.engine.v2.ModifiedColumnSet;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.ShortSingleValueSource;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.engine.v2.TstUtils.*;

public class UpdateCoalescerTest {

    private Listener.Update[] newEmptyUpdates(int numUpdates) {
        final Listener.Update[] ret = new Listener.Update[numUpdates];
        for (int i = 0; i < numUpdates; ++i) {
            ret[i] = new Listener.Update();
            ret[i].added = RowSetFactoryImpl.INSTANCE.empty();
            ret[i].removed = RowSetFactoryImpl.INSTANCE.empty();
            ret[i].modified = RowSetFactoryImpl.INSTANCE.empty();
            ret[i].modifiedColumnSet = ModifiedColumnSet.EMPTY;
            ret[i].shifted = RowSetShiftData.EMPTY;
        }
        return ret;
    }

    @Test
    public void testTrivialMerge() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(2, 4, 6);
        up[1].added = i(3, 5, 7);
        up[0].modified = i(12, 14, 16);
        up[1].modified = i(13, 15, 17);
        up[0].removed = i(22, 24, 26);
        up[1].removed = i(23, 25, 27);

        final RowSet origRowSet = RowSetFactoryImpl.INSTANCE.fromRange(10, 29);
        validateFinalIndex(origRowSet, up);
    }

    private ModifiedColumnSet newMCSForColumns(final String... names) {
        Map<String, ColumnSource<?>> columns = new LinkedHashMap<>();
        for (final String name : names) {
            columns.put(name, new ShortSingleValueSource());
        }
        return new ModifiedColumnSet(columns);
    }

    @Test
    public void testMergeMCSFirstALL() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(4, 5);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].modified = i(6, 7);
        up[1].modifiedColumnSet = newMCSForColumns("A", "B", "C");
        up[1].modifiedColumnSet.setAll("A");

        final RowSet origRowSet = RowSetFactoryImpl.INSTANCE.fromRange(10, 29);
        final Listener.Update agg = validateFinalIndex(origRowSet, up);
        Assert.equals(agg.modifiedColumnSet, "agg.modifiedColumnSet", ModifiedColumnSet.ALL, "ModifiedColumnSet.ALL");
        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6, 7));
    }

    @Test
    public void testMergeMCSSecondALL() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(4, 5);
        up[0].modifiedColumnSet = newMCSForColumns("A", "B", "C");
        up[0].modifiedColumnSet.setAll("A");
        up[1].modified = i(6, 7);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;

        final RowSet origRowSet = RowSetFactoryImpl.INSTANCE.fromRange(10, 29);
        final Listener.Update agg = validateFinalIndex(origRowSet, up);
        Assert.equals(agg.modifiedColumnSet, "agg.modifiedColumnSet", ModifiedColumnSet.ALL, "ModifiedColumnSet.ALL");
        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6, 7));
    }

    @Test
    public void testMergeMCSUnion() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(4, 5);
        up[0].modifiedColumnSet = newMCSForColumns("A", "B", "C");
        up[0].modifiedColumnSet.setAll("C");

        up[1].modified = i(6, 7);
        up[1].modifiedColumnSet = new ModifiedColumnSet(up[0].modifiedColumnSet);
        up[1].modifiedColumnSet.setAll("B");

        final RowSet origRowSet = RowSetFactoryImpl.INSTANCE.fromRange(10, 29);
        final Listener.Update agg = validateFinalIndex(origRowSet, up);
        final ModifiedColumnSet expected = new ModifiedColumnSet(up[0].modifiedColumnSet);
        expected.setAll("B", "C");
        Assert.eqTrue(agg.modifiedColumnSet.containsAll(expected), "agg.modifiedColumnSet.containsAll(expected)");
        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6, 7));
    }

    @Test
    public void testSuppressModifiedAdds() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(10);
        up[1].modified = i(10);

        final RowSet origRowSet = i();
        final Listener.Update agg = validateFinalIndex(origRowSet, up);
        Assert.equals(agg.added, "agg.added", RowSetFactoryImpl.INSTANCE.fromKeys(10),
                "RowSetFactoryImpl.INSTANCE.fromKeys(10)");
        Assert.equals(agg.modified, "agg.modified", RowSetFactoryImpl.INSTANCE.empty(),
                "RowSetFactoryImpl.INSTANCE.empty()");
    }

    @Test
    public void testRemovedAddedRemoved() {
        final Listener.Update[] up = newEmptyUpdates(2);

        up[0].added = i(2);
        up[0].removed = i(2);
        up[1].removed = i(2);

        final RowSet origRowSet = i(2);
        validateFinalIndex(origRowSet, up);
    }

    @Test
    public void testRemovedThenAdded() {
        final Listener.Update[] up = newEmptyUpdates(3);

        up[0].modified = i(2);
        up[1].removed = i(2);
        up[2].added = i(2);

        final RowSet origRowSet = i(2);
        final Listener.Update agg = validateFinalIndex(origRowSet, up);
        Assert.equals(agg.added, "agg.added", RowSetFactoryImpl.INSTANCE.fromKeys(2),
                "RowSetFactoryImpl.INSTANCE.fromKeys(2)");
        Assert.equals(agg.removed, "agg.removed", RowSetFactoryImpl.INSTANCE.fromKeys(2),
                "RowSetFactoryImpl.INSTANCE.fromKeys(2)");
        Assert.equals(agg.modified, "agg.modified", RowSetFactoryImpl.INSTANCE.empty(),
                "RowSetFactoryImpl.INSTANCE.empty()");
    }

    @Test
    public void testAddedThenRemoved() {
        final Listener.Update[] up = newEmptyUpdates(3);

        up[0].added = i(2);
        up[1].modified = i(2);
        up[2].removed = i(2);

        final RowSet origRowSet = i();
        final Listener.Update agg = validateFinalIndex(origRowSet, up);
        Assert.equals(agg.added, "agg.added", RowSetFactoryImpl.INSTANCE.empty(),
                "RowSetFactoryImpl.INSTANCE.empty()");
        Assert.equals(agg.removed, "agg.removed", RowSetFactoryImpl.INSTANCE.empty(),
                "RowSetFactoryImpl.INSTANCE.empty()");
        Assert.equals(agg.modified, "agg.modified", RowSetFactoryImpl.INSTANCE.empty(),
                "RowSetFactoryImpl.INSTANCE.empty()");
    }

    @Test
    public void testShiftRegress1() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].shifted = newShiftDataByTriplets(4, 5, -2);
        up[1].shifted = newShiftDataByTriplets(8, 10, +3, 11, 11, +4);

        final RowSet origRowSet = RowSetFactoryImpl.INSTANCE.fromRange(4, 11);
        validateFinalIndex(origRowSet, up);
    }

    @Test
    public void testShiftOverlapWithEntireEmptyShift() {
        final Listener.Update[] up = newEmptyUpdates(2);
        // original rowSet does not include this value, so it is unambiguous
        up[0].shifted = newShiftDataByTriplets(0, 0, +1); // write 0 to 1
        up[1].shifted = newShiftDataByTriplets(2, 2, -1); // write 2 to 1

        // this test is less bogus than it seems: added 0, shift 0->1, remove 1, and then shift 2->1
        final RowSet pickFirst = i(0, 3);
        validateFinalIndex(pickFirst, up);
        final RowSet pickSecond = i(2, 3);
        validateFinalIndex(pickSecond, up);
    }

    @Test
    public void testShiftPartialOverlap() {
        final Listener.Update[] up = newEmptyUpdates(2);

        up[0].shifted = newShiftDataByTriplets(0, 1, +1);
        up[1].shifted = newShiftDataByTriplets(3, 4, -1);

        final RowSet beforeRowSet = i(0, 1, 4);
        validateFinalIndex(beforeRowSet, up);
        final RowSet afterRowSet = i(0, 3, 4);
        validateFinalIndex(afterRowSet, up);
    }

    @Test
    public void testShiftOverlapViaRecursiveShift() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].shifted = newShiftDataByTriplets(0, 0, +1, 4, 4, -2);
        up[1].shifted = newShiftDataByTriplets(2, 2, -1);

        final RowSet pickFirst = i(0, 5, 7, 8);
        validateFinalIndex(pickFirst, up);
        final RowSet pickSecond = i(4, 5, 7, 8);
        validateFinalIndex(pickSecond, up);
    }

    @Test
    public void testMergeWithDifferentMCS() {
        final ModifiedColumnSet mcsTemplate = newMCSForColumns("A", "B", "C");
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(1, 2, 3);
        up[0].modifiedColumnSet = mcsTemplate.copy();
        up[0].modifiedColumnSet.setAll("A", "B");
        up[1].modified = i(2, 3, 4);
        up[1].modifiedColumnSet = mcsTemplate.copy();
        up[1].modifiedColumnSet.setAll("B", "C");

        final RowSet all = i(0, 1, 2, 3, 4, 5);
        final UpdateCoalescer coalescer = new UpdateCoalescer(all, up[0]);
        for (int i = 1; i < up.length; ++i) {
            coalescer.update(up[i]);
        }
        final Listener.Update agg = coalescer.coalesce();

        Assert.equals(agg.modified, "agg.modified", i(1, 2, 3, 4));
        mcsTemplate.setAllDirty();
        Assert.eqTrue(coalescer.modifiedColumnSet.containsAll(mcsTemplate),
                "coalescer.modifiedColumnSet.containsAll(mcsTemplate)");
    }

    @Test
    public void testRemoveAfterModify() {
        final ModifiedColumnSet mcsTemplate = newMCSForColumns("A", "B", "C");
        final Listener.Update[] up = newEmptyUpdates(4);
        up[0].modified = i(1, 2, 3);
        up[0].modifiedColumnSet = mcsTemplate.copy();
        up[0].modifiedColumnSet.setAll("A", "B");
        up[1].modified = i(2, 3, 4);
        up[1].modifiedColumnSet = mcsTemplate.copy();
        up[1].modifiedColumnSet.setAll("B", "C");
        up[2].added = i(2, 3);
        up[2].removed = i(2, 3);
        up[3].modified = i(2, 3);
        up[3].modifiedColumnSet = mcsTemplate.copy();
        up[3].modifiedColumnSet.setAllDirty();

        final RowSet all = i(0, 1, 2, 3, 4, 5);
        final UpdateCoalescer coalescer = new UpdateCoalescer(all, up[0]);
        for (int i = 1; i < up.length; ++i) {
            coalescer.update(up[i]);
        }
        final Listener.Update agg = coalescer.coalesce();

        Assert.equals(agg.modified, "agg.modified", i(1, 4));
        mcsTemplate.setAllDirty();
        Assert.eqTrue(coalescer.modifiedColumnSet.containsAll(mcsTemplate),
                "coalescer.modifiedColumnSet.containsAll(mcsTemplate)");
    }

    @Test
    public void testShiftAfterModifyDirtyPerColumn() {
        final ModifiedColumnSet mcsTemplate = newMCSForColumns("A", "B", "C");
        final Listener.Update[] up = newEmptyUpdates(3);
        up[0].modified = i(1, 2, 3);
        up[0].modifiedColumnSet = mcsTemplate.copy();
        up[0].modifiedColumnSet.setAll("A");
        up[1].modified = i(3);
        up[1].modifiedColumnSet = mcsTemplate.copy();
        up[1].modifiedColumnSet.setAll("B");
        up[2].added = i(1, 2, 3);
        up[2].modified = i(5);
        up[2].modifiedColumnSet = mcsTemplate.copy();
        up[2].modifiedColumnSet.setAll("C");
        up[2].shifted = newShiftDataByTriplets(1, 3, 3);

        final RowSet all = i(0, 1, 2, 3, 4, 5);
        final UpdateCoalescer coalescer = new UpdateCoalescer(all, up[0]);
        for (int i = 1; i < up.length; ++i) {
            coalescer.update(up[i]);
        }
        final Listener.Update agg = coalescer.coalesce();

        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6));
        mcsTemplate.setAllDirty();
        Assert.eqTrue(coalescer.modifiedColumnSet.containsAll(mcsTemplate),
                "coalescer.modifiedColumnSet.containsAll(mcsTemplate)");
    }

    @Test
    public void testFlattenRegress() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].removed = i(0, 4, 5);
        up[0].modified = i(0, 1, 2, 3);
        up[0].shifted = newShiftDataByTriplets(1, 3, -1, 6, 6, -3);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].added = i(0, 1);
        up[1].removed = i(1, 2, 3);
        up[1].shifted = newShiftDataByTriplets(0, 0, +2);
        up[1].modified = i(2);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;

        final RowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 6);
        validateFinalIndex(rowSet, up);
    }

    @Test
    public void testFlattenRegress2() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].removed = i(0, 3);
        up[0].modified = i(0, 1);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(1, 2, -1);

        up[1].added = i(0, 1, 2);
        up[1].removed = i(1);
        up[1].modified = i(3);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(0, 0, 3);

        final RowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 3);
        validateFinalIndex(rowSet, up);
    }

    @Test
    public void testFlattenRegress3() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(0, 1, 2);
        up[0].removed = i(0, 2);
        up[0].modified = i(3);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(1, 1, +2);
        up[1].added = i(3, 4, 5);
        up[1].removed = i(1);
        up[1].modified = i(0, 1, 2);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(2, 3, -1);

        final RowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 2);
        validateFinalIndex(rowSet, up);
    }

    @Test
    public void testFlattenRegress4() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(1, 2);
        up[0].shifted = newShiftDataByTriplets(1, 1, +2);
        up[1].added = i(1, 2);
        up[1].removed = i(3);
        up[1].modified = i(0, 4);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(1, 2, +2);

        final RowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 1);
        validateFinalIndex(rowSet, up);
    }

    @Test
    public void testFlattenRegress5() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(0, 3);
        up[0].modified = i(1, 2);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(0, 1, +1);
        up[1].added = i(0);
        up[1].removed = i(0, 1, 2);
        up[1].shifted = newShiftDataByTriplets(3, 3, -2);

        final RowSet rowSet = i(0, 1, 4);
        validateFinalIndex(rowSet, up);
    }

    @Test
    public void testFlattenRegress6() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(0, 3);
        up[0].removed = i(1);
        up[0].modified = i(1, 2, 4);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(0, 0, +1, 3, 3, +1);
        up[1].added = i(2, 3, 4, 5, 6);
        up[1].removed = i(1, 3, 4);
        up[1].modified = i(0, 1);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(2, 2, -1);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 3);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
    }

    @Test
    public void testFlattenRegress7() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(0, 1, 2);
        up[0].removed = i(1, 2, 4);
        up[0].modified = i(3, 4);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(0, 0, +3, 3, 3, +1);
        up[1].added = i(3, 4);
        up[1].removed = i(0, 2);
        up[1].modified = i(0, 1);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(1, 1, -1, 3, 4, -2);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 4);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
    }

    @Test
    public void testFlattenRegress8() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(2, 3, 4);
        up[0].removed = i(0, 1, 3);
        up[0].modified = i(0, 1);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(2, 2, -2, 4, 4, -3);
        up[1].added = i(0, 4);
        up[1].modified = i(1, 2, 3, 5, 6);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(0, 2, +1, 3, 4, +2);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 4);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
    }

    @Test
    public void testFlattenRegress9() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(0, 2, 4);
        up[0].removed = i(0);
        up[0].modified = i(1, 3);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(2, 2, 1);
        up[1].removed = i(1, 2);
        up[1].modified = i(0, 1, 2);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(3, 4, -2);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 2);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
    }

    @Test
    public void testFlattenRegress10() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(1, 3, 4);
        up[0].removed = i(2);
        up[0].shifted = newShiftDataByTriplets(1, 1, +1);
        up[1].added = i(4, 5);
        up[1].removed = i(0);
        up[1].shifted = newShiftDataByTriplets(1, 4, -1);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 2);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
    }

    @Test
    public void testFlattenRegress11() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(1, 2);
        up[0].removed = i(1, 5);
        up[0].shifted = newShiftDataByTriplets(2, 4, +1);
        up[1].added = i(1, 2);
        up[1].removed = i(1, 2, 3, 4);
        up[1].shifted = newShiftDataByTriplets(5, 6, -2);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 6);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
    }

    @Test
    public void testFlattenRegress12() {
        final Listener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(2, 3);
        up[0].removed = i(0);
        up[0].shifted = newShiftDataByTriplets(1, 2, -1);
        up[1].added = i(0, 2);
        up[1].removed = i(1, 2);
        up[1].shifted = newShiftDataByTriplets(0, 0, +1);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 6);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
        // The remainder of the first shift conflicts with a now non-shifting element; so there should be no shifts.
        Assert.eq(agg.shifted.size(), "agg.shifted.size()", 0);
    }

    @Test
    public void testFlattenRegress13() {
        final Listener.Update[] up = newEmptyUpdates(4);
        up[0].added = i(2, 4, 5);
        up[0].removed = i(0, 3, 5);
        up[0].shifted = newShiftDataByTriplets(1, 4, -1);
        up[1].added = i(2, 6, 7);
        up[1].removed = i(2);
        up[2].added = i(6, 7, 8, 9, 10);
        up[2].removed = i(0, 1);
        up[2].shifted = newShiftDataByTriplets(2, 7, -2);
        up[3].added = i(7);
        up[3].shifted = newShiftDataByTriplets(7, 10, +1);

        final MutableRowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(0, 5);
        final Listener.Update agg = validateFinalIndex(rowSet, up);
        rowSet.remove(agg.removed);
        agg.shifted.apply(rowSet);
        rowSet.insert(agg.added);
        Assert.eqTrue(rowSet.isFlat(), "rowSet.isFlat()");
    }

    @Test
    public void testSortRegress1() {
        final Listener.Update[] up = newEmptyUpdates(4);
        // {added={1073741825,1073741827-1073741828,1073741832-1073741833}, removed={1073741825-1073741827},
        // modified={1073741829,1073741831}, shifted={[1073741828,1073741828]-2},
        // modifiedColumnSet={Sym,doubleCol,Indices}}
        up[0].added = i(1073741825, 1073741827, 1073741828, 1073741832, 1073741833);
        up[0].removed = i(1073741825, 1073741826, 1073741827);
        up[0].shifted = newShiftDataByTriplets(1073741828, 1073741828, -2);
        // {added={1073741827,1073741832}, removed={1073741825,1073741828,1073741832-1073741833},
        // modified={1073741825-1073741826,1073741829-1073741831}, shifted={[1073741826,1073741827]-1},
        // modifiedColumnSet={Sym}}
        up[1].added = i(1073741827, 1073741832);
        up[1].removed = i(1073741825, 1073741828, 1073741832, 1073741833);
        up[1].shifted = newShiftDataByTriplets(1073741826, 1073741827, -1);
        // {added={1073741823,1073741827,1073741832}, removed={1073741830},
        // modified={1073741824-1073741826,1073741829,1073741831,1073741833},
        // shifted={[1073741825,1073741827]-1,[1073741832,1073741832]+1}, modifiedColumnSet={Sym,intCol,Indices}}
        up[2].added = i(1073741823, 1073741827, 1073741832);
        up[2].removed = i(1073741830);
        up[2].shifted = newShiftDataByTriplets(1073741825, 1073741827, -1, 1073741832, 1073741832, +1);
        // {added={1073741820-1073741823,1073741826,1073741830}, removed={1073741823,1073741827},
        // modified={1073741819,1073741824-1073741825,1073741829,1073741831-1073741833},
        // shifted={[1073741824,1073741824]-5,[1073741825,1073741826]-1}, modifiedColumnSet={intCol,Indices}}
        up[3].added = i(1073741820, 1073741821, 1073741822, 1073741823, 1073741826, 1073741830);
        up[3].removed = i(1073741823, 1073741827);
        up[3].shifted = newShiftDataByTriplets(1073741824, 1073741824, -5, 1073741825, 1073741826, -1);

        final RowSet rowSet = RowSetFactoryImpl.INSTANCE.fromRange(1073741825, 1073741831);
        validateFinalIndex(rowSet, up);
    }

    private RowSetShiftData newShiftDataByTriplets(long... values) {
        Assert.eqTrue(values.length % 3 == 0, "values.length % 3 == 0");

        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
        for (int i = 0; i < values.length; i += 3) {
            builder.shiftRange(values[i], values[i + 1], values[i + 2]);
        }

        return builder.build();
    }

    private Listener.Update validateFinalIndex(final RowSet rowSet, final Listener.Update[] updates) {
        final UpdateCoalescer coalescer = new UpdateCoalescer(rowSet, updates[0]);
        for (int i = 1; i < updates.length; ++i) {
            coalescer.update(updates[i]);
        }
        final Listener.Update agg = coalescer.coalesce();

        try (final MutableRowSet perUpdate = rowSet.clone();
             final MutableRowSet aggUpdate = rowSet.clone();
             final MutableRowSet perModify = RowSetFactoryImpl.INSTANCE.empty();
             final MutableRowSet perAdded = RowSetFactoryImpl.INSTANCE.empty()) {

            for (Listener.Update up : updates) {
                perAdded.remove(up.removed);
                up.shifted.apply(perAdded);
                perAdded.insert(up.added);

                perModify.remove(up.removed);
                up.shifted.apply(perModify);
                perModify.insert(up.modified);

                perUpdate.remove(up.removed);
                up.shifted.apply(perUpdate);
                perUpdate.insert(up.added);
            }

            aggUpdate.remove(agg.removed);
            agg.shifted.apply(aggUpdate);
            aggUpdate.insert(agg.added);

            Assert.equals(perUpdate, "perUpdate", aggUpdate, "aggUpdate");

            perModify.remove(perAdded);
            Assert.equals(perModify, "perModify", agg.modified, "agg.modified");
        }

        // verify that the shift does not overwrite data
        try (final MutableRowSet myindex = rowSet.clone()) {
            myindex.remove(agg.removed);
            agg.shifted.apply(((beginRange, endRange, shiftDelta) -> {
                if (shiftDelta < 0) {
                    final RowSet.SearchIterator iter = myindex.searchIterator();
                    if (!iter.advance(beginRange + shiftDelta)) {
                        return;
                    }
                    Assert.eqTrue(iter.currentValue() >= beginRange, "iter.currentValue() >= beginRange");
                } else {
                    final RowSet.SearchIterator iter = myindex.reverseIterator();
                    if (!iter.advance(endRange + shiftDelta)) {
                        return;
                    }
                    Assert.eqTrue(iter.currentValue() <= endRange, "iter.currentValue() <= endRange");
                }
                try (final RowSet sub = RowSetFactoryImpl.INSTANCE.fromRange(beginRange, endRange);
                     final MutableRowSet moving = myindex.extract(sub)) {
                    moving.shiftInPlace(shiftDelta);
                    myindex.insert(moving);
                }
            }));
        }

        return agg;
    }
}
