/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.by.ssmcountdistinct;

import io.deephaven.db.tables.dbarrays.DbFloatArray;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.ssms.FloatSegmentedSortedMultiset;
import io.deephaven.db.v2.utils.Index;

/**
 * A {@link SsmBackedColumnSource} for Floats.
 */
public class FloatSsmBackedSource extends AbstractColumnSource<DbFloatArray>
                                 implements ColumnSourceGetDefaults.ForObject<DbFloatArray>,
                                            MutableColumnSourceGetDefaults.ForObject<DbFloatArray>,
                                            SsmBackedColumnSource<FloatSegmentedSortedMultiset, DbFloatArray> {
    private final ObjectArraySource<FloatSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public FloatSsmBackedSource() {
        super(DbFloatArray.class, float.class);
        underlying = new ObjectArraySource<>(FloatSegmentedSortedMultiset.class, float.class);
    }
    //endregion Constructor

    //region SsmBackedColumnSource
    @Override
    public FloatSegmentedSortedMultiset getOrCreate(long key) {
        FloatSegmentedSortedMultiset ssm = underlying.getUnsafe(key);
        if(ssm == null) {
            //region CreateNew
            underlying.set(key, ssm = new FloatSegmentedSortedMultiset(DistinctOperatorFactory.NODE_SIZE));
            //endregion CreateNew
        }
        ssm.setTrackDeltas(trackingPrevious);
        return ssm;
    }

    @Override
    public FloatSegmentedSortedMultiset getCurrentSsm(long key) {
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
    public ObjectArraySource<FloatSegmentedSortedMultiset> getUnderlyingSource() {
        return underlying;
    }
    //endregion

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public DbFloatArray get(long index) {
        return underlying.get(index);
    }

    @Override
    public DbFloatArray getPrev(long index) {
        final FloatSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
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
            final FloatSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
