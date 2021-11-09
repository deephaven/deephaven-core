/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.by.ssmcountdistinct;

import io.deephaven.engine.tables.dbarrays.LongVector;
import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.engine.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.engine.v2.sources.ObjectArraySource;
import io.deephaven.engine.v2.ssms.LongSegmentedSortedMultiset;
import io.deephaven.engine.v2.utils.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Longs.
 */
public class LongSsmBackedSource extends AbstractColumnSource<LongVector>
                                 implements ColumnSourceGetDefaults.ForObject<LongVector>,
                                            MutableColumnSourceGetDefaults.ForObject<LongVector>,
                                            SsmBackedColumnSource<LongSegmentedSortedMultiset, LongVector> {
    private final ObjectArraySource<LongSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public LongSsmBackedSource() {
        super(LongVector.class, long.class);
        underlying = new ObjectArraySource<>(LongSegmentedSortedMultiset.class, long.class);
    }
    //endregion Constructor

    //region SsmBackedColumnSource
    @Override
    public LongSegmentedSortedMultiset getOrCreate(long key) {
        LongSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if(ssm == null) {
            //region CreateNew
            underlying.set(key, ssm = new LongSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE));
            //endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public LongSegmentedSortedMultiset getCurrentSsm(long key) {
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
    public ObjectArraySource<LongSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    //endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public LongVector get(long index) {
        return underlying.get(index);
    }

    @Override
    public LongVector getPrev(long index) {
        final LongSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
        return maybePrev == null ? null : maybePrev.getPrevValues();
    }

    @Override
    public void startTrackingPrevValues() {
        trackingPrevious = true;
        underlying.startTrackingPrevValues();
    }

    @Override
    public void clearDeltas(RowSet indices) {
        indices.iterator().forEachLong(key -> {
            final LongSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
