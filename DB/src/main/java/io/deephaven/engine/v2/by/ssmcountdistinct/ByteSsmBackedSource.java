/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsmBackedSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.by.ssmcountdistinct;

import io.deephaven.engine.tables.dbarrays.DbByteArray;
import io.deephaven.engine.v2.sources.AbstractColumnSource;
import io.deephaven.engine.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.engine.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.engine.v2.sources.ObjectArraySource;
import io.deephaven.engine.v2.ssms.ByteSegmentedSortedMultiset;
import io.deephaven.engine.v2.utils.RowSet;

/**
 * A {@link SsmBackedColumnSource} for Bytes.
 */
public class ByteSsmBackedSource extends AbstractColumnSource<DbByteArray>
                                 implements ColumnSourceGetDefaults.ForObject<DbByteArray>,
                                            MutableColumnSourceGetDefaults.ForObject<DbByteArray>,
                                            SsmBackedColumnSource<ByteSegmentedSortedMultiset, DbByteArray> {
    private final ObjectArraySource<ByteSegmentedSortedMultiset> underlying;
    private boolean trackingPrevious = false;

    //region Constructor
    public ByteSsmBackedSource() {
        super(DbByteArray.class, byte.class);
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
    public DbByteArray get(long index) {
        return underlying.get(index);
    }

    @Override
    public DbByteArray getPrev(long index) {
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
