/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.by.ssmcountdistinct;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;

import java.util.Objects;

import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.ssms.ObjectSegmentedSortedMultiset;
import io.deephaven.db.v2.utils.Index;

/**
 * A {@link SsmBackedColumnSource} for Objects.
 */
public class ObjectSsmBackedSource extends AbstractColumnSource<DbArray>
                                 implements ColumnSourceGetDefaults.ForObject<DbArray>,
                                            MutableColumnSourceGetDefaults.ForObject<DbArray>,
                                            SsmBackedColumnSource<ObjectSegmentedSortedMultiset, DbArray> {
    private final ObjectArraySource<ObjectSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public ObjectSsmBackedSource(Class type) {
        super(DbArray.class, type);
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
    public DbArray get(long index) {
        return underlying.get(index);
    }

    @Override
    public DbArray getPrev(long index) {
        final ObjectSegmentedSortedMultiset maybePrev = underlying.getPrev(index);
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
            final ObjectSegmentedSortedMultiset ssm = getCurrentSsm(key);
            if(ssm != null) {
                ssm.clearDeltas();
            }
            return true;
        });
    }
}
