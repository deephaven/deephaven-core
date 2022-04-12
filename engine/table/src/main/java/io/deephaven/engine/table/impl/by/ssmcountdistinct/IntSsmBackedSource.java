/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.vector.IntVector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.IntSegmentedSortedMultiset;
import io.deephaven.engine.rowset.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Integers.
 */
public class IntSsmBackedSource extends AbstractColumnSource<IntVector>
                                 implements ColumnSourceGetDefaults.ForObject<IntVector>,
                                            MutableColumnSourceGetDefaults.ForObject<IntVector>,
                                            SsmBackedColumnSource<IntSegmentedSortedMultiset, IntVector> {
    private final ObjectArraySource<IntSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public IntSsmBackedSource() {
        super(IntVector.class, int.class);
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
    public IntVector get(long index) {
        return underlying.get(index);
    }

    @Override
    public IntVector getPrev(long index) {
        final IntSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
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
            final IntSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
