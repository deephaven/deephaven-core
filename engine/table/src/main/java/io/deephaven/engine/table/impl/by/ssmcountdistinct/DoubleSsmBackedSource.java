/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.vector.DoubleVector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.DoubleSegmentedSortedMultiset;
import io.deephaven.engine.rowset.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Doubles.
 */
public class DoubleSsmBackedSource extends AbstractColumnSource<DoubleVector>
                                 implements ColumnSourceGetDefaults.ForObject<DoubleVector>,
                                            MutableColumnSourceGetDefaults.ForObject<DoubleVector>,
                                            SsmBackedColumnSource<DoubleSegmentedSortedMultiset, DoubleVector> {
    private final ObjectArraySource<DoubleSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public DoubleSsmBackedSource() {
        super(DoubleVector.class, double.class);
        underlying = new ObjectArraySource<>(DoubleSegmentedSortedMultiset.class, double.class);
    }
    //endregion Constructor

    //region SsmBackedColumnSource
    @Override
    public DoubleSegmentedSortedMultiset getOrCreate(long key) {
        DoubleSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if(ssm == null) {
            //region CreateNew
            underlying.set(key, ssm = new DoubleSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE));
            //endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public DoubleSegmentedSortedMultiset getCurrentSsm(long key) {
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
    public ObjectArraySource<DoubleSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    //endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public DoubleVector get(long index) {
        return underlying.get(index);
    }

    @Override
    public DoubleVector getPrev(long index) {
        final DoubleSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
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
            final DoubleSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
