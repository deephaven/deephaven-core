/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.vector.ShortVector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.ShortSegmentedSortedMultiset;
import io.deephaven.engine.rowset.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Shorts.
 */
public class ShortSsmBackedSource extends AbstractColumnSource<ShortVector>
                                 implements ColumnSourceGetDefaults.ForObject<ShortVector>,
                                            MutableColumnSourceGetDefaults.ForObject<ShortVector>,
                                            SsmBackedColumnSource<ShortSegmentedSortedMultiset, ShortVector> {
    private final ObjectArraySource<ShortSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public ShortSsmBackedSource() {
        super(ShortVector.class, short.class);
        underlying = new ObjectArraySource<>(ShortSegmentedSortedMultiset.class, short.class);
    }
    //endregion Constructor

    //region SsmBackedColumnSource
    @Override
    public ShortSegmentedSortedMultiset getOrCreate(long key) {
        ShortSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if(ssm == null) {
            //region CreateNew
            underlying.set(key, ssm = new ShortSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE));
            //endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public ShortSegmentedSortedMultiset getCurrentSsm(long key) {
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
    public ObjectArraySource<ShortSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    //endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public ShortVector get(long index) {
        return underlying.get(index);
    }

    @Override
    public ShortVector getPrev(long index) {
        final ShortSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
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
            final ShortSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
