package io.deephaven.db.v2.by.ssmcountdistinct;

import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.tables.dbarrays.DbArrayDirect;
import io.deephaven.db.tables.dbarrays.DbLongArray;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.MutableColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.ssms.LongSegmentedSortedMultiset;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static io.deephaven.db.tables.utils.DBTimeUtils.nanosToTime;

/**
 * A {@link SsmBackedColumnSource} for Longs.
 */
@SuppressWarnings("rawtypes")
public class DbDateTimeSsmSourceWrapper extends AbstractColumnSource<DbArray>
    implements ColumnSourceGetDefaults.ForObject<DbArray>,
    MutableColumnSourceGetDefaults.ForObject<DbArray> {
    private final LongSsmBackedSource underlying;

    public DbDateTimeSsmSourceWrapper(@NotNull final LongSsmBackedSource underlying) {
        super(DbArray.class, DBDateTime.class);
        this.underlying = underlying;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public DbArray<DBDateTime> get(long index) {
        return new ValueWrapper(underlying.getCurrentSsm(index));
    }

    @Override
    public DbArray<DBDateTime> getPrev(long index) {
        final DbLongArray maybePrev = underlying.getPrev(index);
        if (maybePrev == null) {
            return null;
        }

        if (maybePrev instanceof LongSegmentedSortedMultiset) {
            return new ValueWrapper((LongSegmentedSortedMultiset) maybePrev);
        }

        return ValueWrapper.getPrevValues(maybePrev);
    }

    @Override
    public void startTrackingPrevValues() {
        underlying.startTrackingPrevValues();
    }

    public static class ValueWrapper implements DbArray<DBDateTime> {
        final LongSegmentedSortedMultiset underlying;

        public ValueWrapper(LongSegmentedSortedMultiset underlying) {
            this.underlying = underlying;
        }

        @Override
        public DBDateTime get(long i) {
            return nanosToTime(underlying.get(i));
        }

        @Override
        public DbArray<DBDateTime> subArray(long fromIndexInclusive, long toIndexExclusive) {
            return underlying.subArrayAsDate(fromIndexInclusive, toIndexExclusive);
        }

        @Override
        public DbArray<DBDateTime> subArrayByPositions(long[] positions) {
            return underlying.subArrayByPositionsAsDates(positions);
        }

        @Override
        public DBDateTime[] toArray() {
            return underlying.toDateArray();
        }

        @Override
        public Class<DBDateTime> getComponentType() {
            return DBDateTime.class;
        }

        @Override
        public DBDateTime getPrev(long offset) {
            return underlying.getPrevAsDate(offset);
        }

        @Override
        public Chunk<Attributes.Values> toChunk() {
            return underlying.toDateChunk();
        }

        @Override
        public void fillChunk(WritableChunk destChunk) {
            underlying.fillDateChunk(destChunk);
        }

        @Override
        public boolean isEmpty() {
            return underlying.isEmpty();
        }

        @Override
        public DbArray<DBDateTime> getDirect() {
            return underlying.getDirectAsDate();
        }

        @Override
        public long size() {
            return underlying.size();
        }

        @Override
        public int intSize() {
            return underlying.intSize();
        }

        @Override
        public int intSize(@NotNull String operation) {
            return underlying.intSize(operation);
        }

        public static DbArray<DBDateTime> getPrevValues(DbLongArray previousLongs) {
            final DBDateTime[] asDates = new DBDateTime[previousLongs.intSize()];
            for (int ii = 0; ii < asDates.length; ii++) {
                asDates[ii] = nanosToTime(previousLongs.get(ii));
            }

            return new DbArrayDirect<>(asDates);
        }

        @Override
        public String toString() {
            return underlying.toDateString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ValueWrapper that = (ValueWrapper) o;
            return underlying.equals(that.underlying);
        }

        @Override
        public int hashCode() {
            return Objects.hash(underlying);
        }
    }
}
