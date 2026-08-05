//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmcountdistinct;

import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.ssms.LongSegmentedSortedMultiset;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

import static io.deephaven.time.DateTimeUtils.epochNanosToInstant;

/**
 * A {@link SsmBackedColumnSource} for Instants.
 */
@SuppressWarnings("rawtypes")
public class InstantSsmSourceWrapper extends AbstractColumnSource<ObjectVector>
        implements ColumnSourceGetDefaults.ForObject<ObjectVector>,
        MutableColumnSourceGetDefaults.ForObject<ObjectVector> {
    private final LongSsmBackedSource underlying;

    public InstantSsmSourceWrapper(@NotNull final LongSsmBackedSource underlying) {
        super(ObjectVector.class, Instant.class);
        this.underlying = underlying;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public ObjectVector<Instant> get(long rowKey) {
        return new ValueWrapper(underlying.getCurrentSsm(rowKey));
    }

    @Override
    public ObjectVector<Instant> getPrev(long rowKey) {
        final LongVector maybePrev = underlying.getPrev(rowKey);
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

    public static class ValueWrapper implements ObjectVector<Instant> {
        final LongSegmentedSortedMultiset underlying;

        public ValueWrapper(LongSegmentedSortedMultiset underlying) {
            this.underlying = underlying;
        }

        @Override
        public Instant get(long index) {
            return epochNanosToInstant(underlying.get(index));
        }

        @Override
        public ObjectVector<Instant> subVector(long fromIndexInclusive, long toIndexExclusive) {
            return underlying.subArrayAsInstants(fromIndexInclusive, toIndexExclusive);
        }

        @Override
        public ObjectVector<Instant> subVectorByPositions(long[] positions) {
            return underlying.subArrayByPositionsAsInstants(positions);
        }

        @Override
        public Instant[] toArray() {
            return underlying.toInstantArray();
        }

        @Override
        public Instant[] copyToArray() {
            return toArray();
        }

        @Override
        public Class<Instant> getComponentType() {
            return Instant.class;
        }

        @Override
        public boolean isEmpty() {
            return underlying.isEmpty();
        }

        @Override
        public ObjectVector<Instant> getDirect() {
            return underlying.getDirectAsInstants();
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

        public static ObjectVector<Instant> getPrevValues(LongVector previousLongs) {
            final Instant[] asInstants = new Instant[previousLongs.intSize()];
            for (int ii = 0; ii < asInstants.length; ii++) {
                asInstants[ii] = epochNanosToInstant(previousLongs.get(ii));
            }

            return new ObjectVectorDirect<>(asInstants);
        }

        @Override
        public String toString() {
            return underlying.toInstantString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o instanceof ValueWrapper) {
                return underlying.equals(((ValueWrapper) o).underlying);
            }
            // Equal to any ObjectVector with the same Instants, notably our own getDirect() result.
            return ObjectVector.equals(this, o);
        }

        @Override
        public int hashCode() {
            // Must hash like any equal ObjectVector: ObjectVectorDirect uses this helper; the SSM hashes raw longs.
            return ObjectVector.hashCode(this);
        }
    }
}
