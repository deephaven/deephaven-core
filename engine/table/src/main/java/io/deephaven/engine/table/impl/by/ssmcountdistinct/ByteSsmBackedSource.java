/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.vector.ByteVector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.ByteSegmentedSortedMultiset;
import io.deephaven.engine.rowset.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Bytes.
 */
public class ByteSsmBackedSource extends AbstractColumnSource<ByteVector>
                                 implements ColumnSourceGetDefaults.ForObject<ByteVector>,
                                            MutableColumnSourceGetDefaults.ForObject<ByteVector>,
                                            SsmBackedColumnSource<ByteSegmentedSortedMultiset, ByteVector> {
    private final ObjectArraySource<ByteSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public ByteSsmBackedSource() {
        super(ByteVector.class, byte.class);
        underlying = new ObjectArraySource<>(ByteSegmentedSortedMultiset.class, byte.class);
    }
    //endregion Constructor

    //region SsmBackedColumnSource
    @Override
    public ByteSegmentedSortedMultiset getOrCreate(long key) {
        ByteSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if(ssm == null) {
            //region CreateNew
            underlying.set(key, ssm = new ByteSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE));
            //endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public ByteSegmentedSortedMultiset getCurrentSsm(long key) {
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
    public ObjectArraySource<ByteSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    //endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public ByteVector get(long index) {
        return underlying.get(index);
    }

    @Override
    public ByteVector getPrev(long index) {
        final ByteSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
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
            final ByteSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
