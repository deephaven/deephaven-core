package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ShortSingleValueSource;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.db.v2.TstUtils.*;

public class IndexCoalescerTest {

    private ShiftAwareListener.Update[] newEmptyUpdates(int numUpdates) {
        final ShiftAwareListener.Update[] ret = new ShiftAwareListener.Update[numUpdates];
        for (int i = 0; i < numUpdates; ++i) {
            ret[i] = new ShiftAwareListener.Update();
            ret[i].added = Index.CURRENT_FACTORY.getEmptyIndex();
            ret[i].removed = Index.CURRENT_FACTORY.getEmptyIndex();
            ret[i].modified = Index.CURRENT_FACTORY.getEmptyIndex();
            ret[i].modifiedColumnSet = ModifiedColumnSet.EMPTY;
            ret[i].shifted = IndexShiftData.EMPTY;
        }
        return ret;
    }

    @Test
    public void testTrivialMerge() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(2, 4, 6);
        up[1].added = i(3, 5, 7);
        up[0].modified = i(12, 14, 16);
        up[1].modified = i(13, 15, 17);
        up[0].removed = i(22, 24, 26);
        up[1].removed = i(23, 25, 27);

        final Index origIndex = Index.CURRENT_FACTORY.getIndexByRange(10, 29);
        validateFinalIndex(origIndex, up);
    }

    private ModifiedColumnSet newMCSForColumns(final String... names) {
        Map<String, ColumnSource> columns = new LinkedHashMap<>();
        for (final String name : names) {
            columns.put(name, new ShortSingleValueSource());
        }
        return new ModifiedColumnSet(columns);
    }

    @Test
    public void testMergeMCSFirstALL() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(4, 5);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].modified = i(6, 7);
        up[1].modifiedColumnSet = newMCSForColumns("A", "B", "C");
        up[1].modifiedColumnSet.setAll("A");

        final Index origIndex = Index.CURRENT_FACTORY.getIndexByRange(10, 29);
        final ShiftAwareListener.Update agg = validateFinalIndex(origIndex, up);
        Assert.equals(agg.modifiedColumnSet, "agg.modifiedColumnSet", ModifiedColumnSet.ALL,
            "ModifiedColumnSet.ALL");
        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6, 7));
    }

    @Test
    public void testMergeMCSSecondALL() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(4, 5);
        up[0].modifiedColumnSet = newMCSForColumns("A", "B", "C");
        up[0].modifiedColumnSet.setAll("A");
        up[1].modified = i(6, 7);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;

        final Index origIndex = Index.CURRENT_FACTORY.getIndexByRange(10, 29);
        final ShiftAwareListener.Update agg = validateFinalIndex(origIndex, up);
        Assert.equals(agg.modifiedColumnSet, "agg.modifiedColumnSet", ModifiedColumnSet.ALL,
            "ModifiedColumnSet.ALL");
        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6, 7));
    }

    @Test
    public void testMergeMCSUnion() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(4, 5);
        up[0].modifiedColumnSet = newMCSForColumns("A", "B", "C");
        up[0].modifiedColumnSet.setAll("C");

        up[1].modified = i(6, 7);
        up[1].modifiedColumnSet = new ModifiedColumnSet(up[0].modifiedColumnSet);
        up[1].modifiedColumnSet.setAll("B");

        final Index origIndex = Index.CURRENT_FACTORY.getIndexByRange(10, 29);
        final ShiftAwareListener.Update agg = validateFinalIndex(origIndex, up);
        final ModifiedColumnSet expected = new ModifiedColumnSet(up[0].modifiedColumnSet);
        expected.setAll("B", "C");
        Assert.eqTrue(agg.modifiedColumnSet.containsAll(expected),
            "agg.modifiedColumnSet.containsAll(expected)");
        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6, 7));
    }

    @Test
    public void testSuppressModifiedAdds() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(10);
        up[1].modified = i(10);

        final Index origIndex = i();
        final ShiftAwareListener.Update agg = validateFinalIndex(origIndex, up);
        Assert.equals(agg.added, "agg.added", Index.CURRENT_FACTORY.getIndexByValues(10),
            "Index.CURRENT_FACTORY.getIndexByValues(10)");
        Assert.equals(agg.modified, "agg.modified", Index.CURRENT_FACTORY.getEmptyIndex(),
            "Index.CURRENT_FACTORY.getEmptyIndex()");
    }

    @Test
    public void testRemovedAddedRemoved() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);

        up[0].added = i(2);
        up[0].removed = i(2);
        up[1].removed = i(2);

        final Index origIndex = i(2);
        validateFinalIndex(origIndex, up);
    }

    @Test
    public void testRemovedThenAdded() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(3);

        up[0].modified = i(2);
        up[1].removed = i(2);
        up[2].added = i(2);

        final Index origIndex = i(2);
        final ShiftAwareListener.Update agg = validateFinalIndex(origIndex, up);
        Assert.equals(agg.added, "agg.added", Index.CURRENT_FACTORY.getIndexByValues(2),
            "Index.CURRENT_FACTORY.getIndexByValues(2)");
        Assert.equals(agg.removed, "agg.removed", Index.CURRENT_FACTORY.getIndexByValues(2),
            "Index.CURRENT_FACTORY.getIndexByValues(2)");
        Assert.equals(agg.modified, "agg.modified", Index.CURRENT_FACTORY.getEmptyIndex(),
            "Index.CURRENT_FACTORY.getEmptyIndex()");
    }

    @Test
    public void testAddedThenRemoved() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(3);

        up[0].added = i(2);
        up[1].modified = i(2);
        up[2].removed = i(2);

        final Index origIndex = i();
        final ShiftAwareListener.Update agg = validateFinalIndex(origIndex, up);
        Assert.equals(agg.added, "agg.added", Index.CURRENT_FACTORY.getEmptyIndex(),
            "Index.CURRENT_FACTORY.getEmptyIndex()");
        Assert.equals(agg.removed, "agg.removed", Index.CURRENT_FACTORY.getEmptyIndex(),
            "Index.CURRENT_FACTORY.getEmptyIndex()");
        Assert.equals(agg.modified, "agg.modified", Index.CURRENT_FACTORY.getEmptyIndex(),
            "Index.CURRENT_FACTORY.getEmptyIndex()");
    }

    @Test
    public void testShiftRegress1() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].shifted = newShiftDataByTriplets(4, 5, -2);
        up[1].shifted = newShiftDataByTriplets(8, 10, +3, 11, 11, +4);

        final Index origIndex = Index.CURRENT_FACTORY.getIndexByRange(4, 11);
        validateFinalIndex(origIndex, up);
    }

    @Test
    public void testShiftOverlapWithEntireEmptyShift() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        // original index does not include this value, so it is unambiguous
        up[0].shifted = newShiftDataByTriplets(0, 0, +1); // write 0 to 1
        up[1].shifted = newShiftDataByTriplets(2, 2, -1); // write 2 to 1

        // this test is less bogus than it seems: added 0, shift 0->1, remove 1, and then shift 2->1
        final Index pickFirst = i(0, 3);
        validateFinalIndex(pickFirst, up);
        final Index pickSecond = i(2, 3);
        validateFinalIndex(pickSecond, up);
    }

    @Test
    public void testShiftPartialOverlap() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);

        up[0].shifted = newShiftDataByTriplets(0, 1, +1);
        up[1].shifted = newShiftDataByTriplets(3, 4, -1);

        final Index beforeIndex = i(0, 1, 4);
        validateFinalIndex(beforeIndex, up);
        final Index afterIndex = i(0, 3, 4);
        validateFinalIndex(afterIndex, up);
    }

    @Test
    public void testShiftOverlapViaRecursiveShift() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].shifted = newShiftDataByTriplets(0, 0, +1, 4, 4, -2);
        up[1].shifted = newShiftDataByTriplets(2, 2, -1);

        final Index pickFirst = i(0, 5, 7, 8);
        validateFinalIndex(pickFirst, up);
        final Index pickSecond = i(4, 5, 7, 8);
        validateFinalIndex(pickSecond, up);
    }

    @Test
    public void testMergeWithDifferentMCS() {
        final ModifiedColumnSet mcsTemplate = newMCSForColumns("A", "B", "C");
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].modified = i(1, 2, 3);
        up[0].modifiedColumnSet = mcsTemplate.copy();
        up[0].modifiedColumnSet.setAll("A", "B");
        up[1].modified = i(2, 3, 4);
        up[1].modifiedColumnSet = mcsTemplate.copy();
        up[1].modifiedColumnSet.setAll("B", "C");

        final Index all = i(0, 1, 2, 3, 4, 5);
        final Index.IndexUpdateCoalescer coalescer = new Index.IndexUpdateCoalescer(all, up[0]);
        for (int i = 1; i < up.length; ++i) {
            coalescer.update(up[i]);
        }
        final ShiftAwareListener.Update agg = coalescer.coalesce();

        Assert.equals(agg.modified, "agg.modified", i(1, 2, 3, 4));
        mcsTemplate.setAllDirty();
        Assert.eqTrue(coalescer.modifiedColumnSet.containsAll(mcsTemplate),
            "coalescer.modifiedColumnSet.containsAll(mcsTemplate)");
    }

    @Test
    public void testRemoveAfterModify() {
        final ModifiedColumnSet mcsTemplate = newMCSForColumns("A", "B", "C");
        final ShiftAwareListener.Update[] up = newEmptyUpdates(4);
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

        final Index all = i(0, 1, 2, 3, 4, 5);
        final Index.IndexUpdateCoalescer coalescer = new Index.IndexUpdateCoalescer(all, up[0]);
        for (int i = 1; i < up.length; ++i) {
            coalescer.update(up[i]);
        }
        final ShiftAwareListener.Update agg = coalescer.coalesce();

        Assert.equals(agg.modified, "agg.modified", i(1, 4));
        mcsTemplate.setAllDirty();
        Assert.eqTrue(coalescer.modifiedColumnSet.containsAll(mcsTemplate),
            "coalescer.modifiedColumnSet.containsAll(mcsTemplate)");
    }

    @Test
    public void testShiftAfterModifyDirtyPerColumn() {
        final ModifiedColumnSet mcsTemplate = newMCSForColumns("A", "B", "C");
        final ShiftAwareListener.Update[] up = newEmptyUpdates(3);
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

        final Index all = i(0, 1, 2, 3, 4, 5);
        final Index.IndexUpdateCoalescer coalescer = new Index.IndexUpdateCoalescer(all, up[0]);
        for (int i = 1; i < up.length; ++i) {
            coalescer.update(up[i]);
        }
        final ShiftAwareListener.Update agg = coalescer.coalesce();

        Assert.equals(agg.modified, "agg.modified", i(4, 5, 6));
        mcsTemplate.setAllDirty();
        Assert.eqTrue(coalescer.modifiedColumnSet.containsAll(mcsTemplate),
            "coalescer.modifiedColumnSet.containsAll(mcsTemplate)");
    }

    @Test
    public void testFlattenRegress() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].removed = i(0, 4, 5);
        up[0].modified = i(0, 1, 2, 3);
        up[0].shifted = newShiftDataByTriplets(1, 3, -1, 6, 6, -3);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].added = i(0, 1);
        up[1].removed = i(1, 2, 3);
        up[1].shifted = newShiftDataByTriplets(0, 0, +2);
        up[1].modified = i(2);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 6);
        validateFinalIndex(index, up);
    }

    @Test
    public void testFlattenRegress2() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].removed = i(0, 3);
        up[0].modified = i(0, 1);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(1, 2, -1);

        up[1].added = i(0, 1, 2);
        up[1].removed = i(1);
        up[1].modified = i(3);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(0, 0, 3);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 3);
        validateFinalIndex(index, up);
    }

    @Test
    public void testFlattenRegress3() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
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

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 2);
        validateFinalIndex(index, up);
    }

    @Test
    public void testFlattenRegress4() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(1, 2);
        up[0].shifted = newShiftDataByTriplets(1, 1, +2);
        up[1].added = i(1, 2);
        up[1].removed = i(3);
        up[1].modified = i(0, 4);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(1, 2, +2);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 1);
        validateFinalIndex(index, up);
    }

    @Test
    public void testFlattenRegress5() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(0, 3);
        up[0].modified = i(1, 2);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(0, 1, +1);
        up[1].added = i(0);
        up[1].removed = i(0, 1, 2);
        up[1].shifted = newShiftDataByTriplets(3, 3, -2);

        final Index index = i(0, 1, 4);
        validateFinalIndex(index, up);
    }

    @Test
    public void testFlattenRegress6() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
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

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 3);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
    }

    @Test
    public void testFlattenRegress7() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
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

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 4);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
    }

    @Test
    public void testFlattenRegress8() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(2, 3, 4);
        up[0].removed = i(0, 1, 3);
        up[0].modified = i(0, 1);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(2, 2, -2, 4, 4, -3);
        up[1].added = i(0, 4);
        up[1].modified = i(1, 2, 3, 5, 6);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(0, 2, +1, 3, 4, +2);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 4);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
    }

    @Test
    public void testFlattenRegress9() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(0, 2, 4);
        up[0].removed = i(0);
        up[0].modified = i(1, 3);
        up[0].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[0].shifted = newShiftDataByTriplets(2, 2, 1);
        up[1].removed = i(1, 2);
        up[1].modified = i(0, 1, 2);
        up[1].modifiedColumnSet = ModifiedColumnSet.ALL;
        up[1].shifted = newShiftDataByTriplets(3, 4, -2);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 2);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
    }

    @Test
    public void testFlattenRegress10() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(1, 3, 4);
        up[0].removed = i(2);
        up[0].shifted = newShiftDataByTriplets(1, 1, +1);
        up[1].added = i(4, 5);
        up[1].removed = i(0);
        up[1].shifted = newShiftDataByTriplets(1, 4, -1);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 2);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
    }

    @Test
    public void testFlattenRegress11() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(1, 2);
        up[0].removed = i(1, 5);
        up[0].shifted = newShiftDataByTriplets(2, 4, +1);
        up[1].added = i(1, 2);
        up[1].removed = i(1, 2, 3, 4);
        up[1].shifted = newShiftDataByTriplets(5, 6, -2);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 6);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
    }

    @Test
    public void testFlattenRegress12() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(2);
        up[0].added = i(2, 3);
        up[0].removed = i(0);
        up[0].shifted = newShiftDataByTriplets(1, 2, -1);
        up[1].added = i(0, 2);
        up[1].removed = i(1, 2);
        up[1].shifted = newShiftDataByTriplets(0, 0, +1);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 6);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
        // The remainder of the first shift conflicts with a now non-shifting element; so there
        // should be no shifts.
        Assert.eq(agg.shifted.size(), "agg.shifted.size()", 0);
    }

    @Test
    public void testFlattenRegress13() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(4);
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

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(0, 5);
        final ShiftAwareListener.Update agg = validateFinalIndex(index, up);
        index.remove(agg.removed);
        agg.shifted.apply(index);
        index.insert(agg.added);
        Assert.eqTrue(index.isFlat(), "index.isFlat()");
    }

    @Test
    public void testSortRegress1() {
        final ShiftAwareListener.Update[] up = newEmptyUpdates(4);
        // {added={1073741825,1073741827-1073741828,1073741832-1073741833},
        // removed={1073741825-1073741827}, modified={1073741829,1073741831},
        // shifted={[1073741828,1073741828]-2}, modifiedColumnSet={Sym,doubleCol,Keys}}
        up[0].added = i(1073741825, 1073741827, 1073741828, 1073741832, 1073741833);
        up[0].removed = i(1073741825, 1073741826, 1073741827);
        up[0].shifted = newShiftDataByTriplets(1073741828, 1073741828, -2);
        // {added={1073741827,1073741832}, removed={1073741825,1073741828,1073741832-1073741833},
        // modified={1073741825-1073741826,1073741829-1073741831},
        // shifted={[1073741826,1073741827]-1}, modifiedColumnSet={Sym}}
        up[1].added = i(1073741827, 1073741832);
        up[1].removed = i(1073741825, 1073741828, 1073741832, 1073741833);
        up[1].shifted = newShiftDataByTriplets(1073741826, 1073741827, -1);
        // {added={1073741823,1073741827,1073741832}, removed={1073741830},
        // modified={1073741824-1073741826,1073741829,1073741831,1073741833},
        // shifted={[1073741825,1073741827]-1,[1073741832,1073741832]+1},
        // modifiedColumnSet={Sym,intCol,Keys}}
        up[2].added = i(1073741823, 1073741827, 1073741832);
        up[2].removed = i(1073741830);
        up[2].shifted =
            newShiftDataByTriplets(1073741825, 1073741827, -1, 1073741832, 1073741832, +1);
        // {added={1073741820-1073741823,1073741826,1073741830}, removed={1073741823,1073741827},
        // modified={1073741819,1073741824-1073741825,1073741829,1073741831-1073741833},
        // shifted={[1073741824,1073741824]-5,[1073741825,1073741826]-1},
        // modifiedColumnSet={intCol,Keys}}
        up[3].added = i(1073741820, 1073741821, 1073741822, 1073741823, 1073741826, 1073741830);
        up[3].removed = i(1073741823, 1073741827);
        up[3].shifted =
            newShiftDataByTriplets(1073741824, 1073741824, -5, 1073741825, 1073741826, -1);

        final Index index = Index.CURRENT_FACTORY.getIndexByRange(1073741825, 1073741831);
        validateFinalIndex(index, up);
    }

    private IndexShiftData newShiftDataByTriplets(long... values) {
        Assert.eqTrue(values.length % 3 == 0, "values.length % 3 == 0");

        final IndexShiftData.Builder builder = new IndexShiftData.Builder();
        for (int i = 0; i < values.length; i += 3) {
            builder.shiftRange(values[i], values[i + 1], values[i + 2]);
        }

        return builder.build();
    }

    private ShiftAwareListener.Update validateFinalIndex(final Index index,
        final ShiftAwareListener.Update[] updates) {
        final Index.IndexUpdateCoalescer coalescer =
            new Index.IndexUpdateCoalescer(index, updates[0]);
        for (int i = 1; i < updates.length; ++i) {
            coalescer.update(updates[i]);
        }
        final ShiftAwareListener.Update agg = coalescer.coalesce();

        try (final Index perUpdate = index.clone();
            final Index aggUpdate = index.clone();
            final Index perModify = Index.CURRENT_FACTORY.getEmptyIndex();
            final Index perAdded = Index.CURRENT_FACTORY.getEmptyIndex()) {

            for (ShiftAwareListener.Update up : updates) {
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
        try (final Index myindex = index.clone()) {
            myindex.remove(agg.removed);
            agg.shifted.apply(((beginRange, endRange, shiftDelta) -> {
                if (shiftDelta < 0) {
                    final Index.SearchIterator iter = myindex.searchIterator();
                    if (!iter.advance(beginRange + shiftDelta)) {
                        return;
                    }
                    Assert.eqTrue(iter.currentValue() >= beginRange,
                        "iter.currentValue() >= beginRange");
                } else {
                    final Index.SearchIterator iter = myindex.reverseIterator();
                    if (!iter.advance(endRange + shiftDelta)) {
                        return;
                    }
                    Assert.eqTrue(iter.currentValue() <= endRange,
                        "iter.currentValue() <= endRange");
                }
                try (final Index sub = Index.CURRENT_FACTORY.getIndexByRange(beginRange, endRange);
                    final Index moving = myindex.extract(sub)) {
                    moving.shiftInPlace(shiftDelta);
                    myindex.insert(moving);
                }
            }));
        }

        return agg;
    }
}
