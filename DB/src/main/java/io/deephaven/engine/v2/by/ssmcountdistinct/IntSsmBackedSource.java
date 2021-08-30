/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.by.ssmcountdistinct;

import io.deephaven.engine.structures.vector.DbIntArray;
import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.engine.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.engine.v2.sources.ObjectArraySource;
import io.deephaven.engine.v2.ssms.IntSegmentedSortedMultiset;
import io.deephaven.engine.structures.rowset.Index;

/**
 * A {@link SsmBackedColumnSource} for Integers.
 */
public class IntSsmBackedSource extends AbstractColumnSource<DbIntArray>
                                 implements ColumnSourceGetDefaults.ForObject<DbIntArray>,
                                            MutableColumnSourceGetDefaults.ForObject<DbIntArray>,
                                            SsmBackedColumnSource<IntSegmentedSortedMultiset, DbIntArray> {
    private final ObjectArraySource<IntSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public IntSsmBackedSource() {
        super(DbIntArray.class, int.class);
        underlying = new ObjectArraySource<>(IntSegmentedSortedMultiset.class, int.class);
    }
    //endregion Constructor

    //region SsmBackedColumnSource
    @Override
    public IntSegmentedSortedMultiset getOrCreate(long key) {
        IntSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if(ssm == null) {
            //region CreateNew
            underlying.set(key, ssm = new IntSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE));
            //endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public IntSegmentedSortedMultiset getCurrentSsm(long key) {
        return underlying.getUnsafe(key);
    }

    @Override
    public void clear(long key) {
        underlying.set(key, null);
    }

    @Override
    public void ensureCapacity(long capacity) {
        underlying.ensureCapacity(capacity);
    }

    @Override
    public ObjectArraySource<IntSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    //endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public DbIntArray get(long index) {
        return underlying.get(index);
    }

    @Override
    public DbIntArray getPrev(long index) {
        final IntSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
        return maybePrev == null ? null : maybePrev.getPrevValues();
    }

    @Override
    public void startTrackingPrevValues() {
        trackingPrevious = true;
        underlying.startTrackingPrevValues();
    }

    @Override
    public void clearDeltas(Index indices) {
        indices.iterator().forEachLong(key -> {
            final IntSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
