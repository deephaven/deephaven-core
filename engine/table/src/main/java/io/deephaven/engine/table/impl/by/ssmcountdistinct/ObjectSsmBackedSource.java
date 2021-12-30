/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import java.util.Objects;

import io.deephaven.vector.ObjectVector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.engine.rowset.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Objects.
 */
public class ObjectSsmBackedSource extends AbstractColumnSource<ObjectVector>
                                 implements ColumnSourceGetDefaults.ForObject<ObjectVector>,
                                            MutableColumnSourceGetDefaults.ForObject<ObjectVector>,
                                            SsmBackedColumnSource<ObjectSegmentedSortedMultiset, ObjectVector> {
    private final ObjectArraySource<ObjectSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public ObjectSsmBackedSource(Class type) {
        super(ObjectVector.class, type);
        underlying = new ObjectArraySource<>(ObjectSegmentedSortedMultiset.class, type);
    }
    //endregion Constructor

    //region SsmBackedColumnSource
    @Override
    public ObjectSegmentedSortedMultiset getOrCreate(long key) {
        ObjectSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if(ssm == null) {
            //region CreateNew
            underlying.set(key, ssm = new ObjectSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE, Object.class));
            //endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public ObjectSegmentedSortedMultiset getCurrentSsm(long key) {
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
    public ObjectArraySource<ObjectSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    //endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public ObjectVector get(long index) {
        return underlying.get(index);
    }

    @Override
    public ObjectVector getPrev(long index) {
        final ObjectSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
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
            final ObjectSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
