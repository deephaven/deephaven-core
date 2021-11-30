package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.ssms.LongSegmentedSortedMultiset;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static io.deephaven.time.DateTimeUtils.nanosToTime;

/**
 * A {@link SsmBackedColumnSource} for Longs.
 */
@SuppressWarnings("rawtypes")
public class DateTimeSsmSourceWrapper extends AbstractColumnSource<ObjectVector>
        implements ColumnSourceGetDefaults.ForObject<ObjectVector>,
        MutableColumnSourceGetDefaults.ForObject<ObjectVector> {
    private final LongSsmBackedSource underlying;

    public DateTimeSsmSourceWrapper(@NotNull final LongSsmBackedSource underlying) {
        super(ObjectVector.class, DateTime.class);
        this.underlying = underlying;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public ObjectVector<DateTime> get(long index) {
        return new ValueWrapper(underlying.getCurrentSsm(index));
    }

    @Override
    public ObjectVector<DateTime> getPrev(long index) {
        final LongVector maybePrev = underlying.getPrev(index);
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

    public static class ValueWrapper implements ObjectVector<DateTime> {
        final LongSegmentedSortedMultiset underlying;

        public ValueWrapper(LongSegmentedSortedMultiset underlying) {
            this.underlying = underlying;
        }

        @Override
        public DateTime get(long i) {
            return nanosToTime(underlying.get(i));
        }

        @Override
        public ObjectVector<DateTime> subVector(long fromIndexInclusive, long toIndexExclusive) {
            return underlying.subArrayAsDate(fromIndexInclusive, toIndexExclusive);
        }

        @Override
        public ObjectVector<DateTime> subVectorByPositions(long[] positions) {
            return underlying.subArrayByPositionsAsDates(positions);
        }

        @Override
        public DateTime[] toArray() {
            return underlying.toDateArray();
        }

        @Override
        public Class<DateTime> getComponentType() {
            return DateTime.class;
        }

        @Override
        public boolean isEmpty() {
            return underlying.isEmpty();
        }

        @Override
        public ObjectVector<DateTime> getDirect() {
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

        public static ObjectVector<DateTime> getPrevValues(LongVector previousLongs) {
            final DateTime[] asDates = new DateTime[previousLongs.intSize()];
            for (int ii = 0; ii < asDates.length; ii++) {
                asDates[ii] = nanosToTime(previousLongs.get(ii));
            }

            return new ObjectVectorDirect<>(asDates);
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
