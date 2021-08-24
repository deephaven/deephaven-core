package io.deephaven.db.v2.by;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.util.NullSafeAddition;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.deephaven.db.v2.by.ComboAggregateFactory.*;

abstract class FpChunkedNonNormalCounter {
    // individual state values for nan, positive infinity, and negative infinity
    private LongArraySource nanCount;
    private LongArraySource positiveInfinityCount;
    private LongArraySource negativeInfinityCount;
    // how many states, recorded so we can lazily ensureCapacity nan and infinities
    private long capacity;
    // global flags for whether the nans, positive, and negative infinities are present
    private boolean hasPositiveInfinities = false;
    private boolean hasNegativeInfinities = false;
    private boolean hasNans = false;
    private boolean trackingPrev = false;

    final long updateNanCount(long destination, int newNans) {
        if (newNans > 0 && !hasNans) {
            nanCount = new LongArraySource();
            nanCount.ensureCapacity(capacity);
            if (trackingPrev) {
                nanCount.startTrackingPrevValues();
            }
            hasNans = true;
        }
        final long totalNanCount;
        if (hasNans) {
            totalNanCount = NullSafeAddition.plusLong(nanCount.getUnsafe(destination), newNans);
            if (newNans != 0) {
                nanCount.set(destination, totalNanCount);
            }
        } else {
            totalNanCount = 0;
        }
        return totalNanCount;
    }

    final long updateNanCount(long destination, int oldNans, int newNans) {
        if (newNans == oldNans) {
            if (hasNans) {
                return nanCount.getUnsafe(destination);
            } else {
                return 0;
            }
        }

        if (!hasNans) {
            nanCount = new LongArraySource();
            nanCount.ensureCapacity(capacity);
            if (trackingPrev) {
                nanCount.startTrackingPrevValues();
            }
            hasNans = true;
        }

        final long totalNanCount;
        totalNanCount =
            NullSafeAddition.plusLong(nanCount.getUnsafe(destination), newNans - oldNans);
        nanCount.set(destination, totalNanCount);
        return totalNanCount;
    }

    final long updatePositiveInfinityCount(long destination, int newPositiveInfinity) {
        if (newPositiveInfinity > 0 && !hasPositiveInfinities) {
            positiveInfinityCount = new LongArraySource();
            positiveInfinityCount.ensureCapacity(capacity);
            if (trackingPrev) {
                positiveInfinityCount.startTrackingPrevValues();
            }
            hasPositiveInfinities = true;
        }
        final long totalPositiveInfinityCount;
        if (hasPositiveInfinities) {
            totalPositiveInfinityCount = NullSafeAddition
                .plusLong(positiveInfinityCount.getUnsafe(destination), newPositiveInfinity);
            if (newPositiveInfinity != 0) {
                positiveInfinityCount.set(destination, totalPositiveInfinityCount);
            }
        } else {
            totalPositiveInfinityCount = 0;
        }
        return totalPositiveInfinityCount;
    }


    final long updatePositiveInfinityCount(long destination, int oldPositiveInfinities,
        int newPositiveInfinities) {
        if (newPositiveInfinities == oldPositiveInfinities) {
            if (hasPositiveInfinities) {
                return positiveInfinityCount.getUnsafe(destination);
            } else {
                return 0;
            }
        }

        if (!hasPositiveInfinities) {
            positiveInfinityCount = new LongArraySource();
            positiveInfinityCount.ensureCapacity(capacity);
            if (trackingPrev) {
                positiveInfinityCount.startTrackingPrevValues();
            }
            hasPositiveInfinities = true;
        }

        final long totalPositiveInfinityCount;
        totalPositiveInfinityCount =
            NullSafeAddition.plusLong(positiveInfinityCount.getUnsafe(destination),
                newPositiveInfinities - oldPositiveInfinities);
        positiveInfinityCount.set(destination, totalPositiveInfinityCount);
        return totalPositiveInfinityCount;
    }

    final long updateNegativeInfinityCount(long destination, int newNegativeInfinity) {
        if (newNegativeInfinity > 0 && !hasNegativeInfinities) {
            negativeInfinityCount = new LongArraySource();
            negativeInfinityCount.ensureCapacity(capacity);
            if (trackingPrev) {
                negativeInfinityCount.startTrackingPrevValues();
            }
            hasNegativeInfinities = true;
        }
        final long totalNegativeInfinityCount;
        if (hasNegativeInfinities) {
            totalNegativeInfinityCount = NullSafeAddition
                .plusLong(negativeInfinityCount.getUnsafe(destination), newNegativeInfinity);
            if (newNegativeInfinity != 0) {
                negativeInfinityCount.set(destination, totalNegativeInfinityCount);
            }
        } else {
            totalNegativeInfinityCount = 0;
        }
        return totalNegativeInfinityCount;
    }

    final long updateNegativeInfinityCount(long destination, int oldNegativeInfinities,
        int newNegativeInfinities) {
        if (newNegativeInfinities == oldNegativeInfinities) {
            if (hasNegativeInfinities) {
                return negativeInfinityCount.getUnsafe(destination);
            } else {
                return 0;
            }
        }

        if (!hasNegativeInfinities) {
            negativeInfinityCount = new LongArraySource();
            negativeInfinityCount.ensureCapacity(capacity);
            if (trackingPrev) {
                negativeInfinityCount.startTrackingPrevValues();
            }
            hasNegativeInfinities = true;
        }

        final long totalNegativeInfinityCount;
        totalNegativeInfinityCount =
            NullSafeAddition.plusLong(negativeInfinityCount.getUnsafe(destination),
                newNegativeInfinities - oldNegativeInfinities);
        negativeInfinityCount.set(destination, totalNegativeInfinityCount);
        return totalNegativeInfinityCount;
    }

    final void ensureNonNormalCapacity(long tableSize) {
        capacity = tableSize;
        if (hasNans) {
            nanCount.ensureCapacity(tableSize);
        }
        if (hasPositiveInfinities) {
            positiveInfinityCount.ensureCapacity(tableSize);
        }
        if (hasNegativeInfinities) {
            negativeInfinityCount.ensureCapacity(tableSize);
        }
    }

    void startTrackingPrevFpCounterValues() {
        trackingPrev = true;
        if (nanCount != null) {
            nanCount.startTrackingPrevValues();
        }
        if (positiveInfinityCount != null) {
            positiveInfinityCount.startTrackingPrevValues();
        }
        if (negativeInfinityCount != null) {
            negativeInfinityCount.startTrackingPrevValues();
        }
    }

    Map<String, ColumnSource<?>> fpInternalColumnSources(final String name) {
        final Map<String, ColumnSource<?>> results = new LinkedHashMap<>();
        if (nanCount != null) {
            results.put(name + ROLLUP_NAN_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, nanCount);
        } else {
            results.put(name + ROLLUP_NAN_COLUMN_ID + ROLLUP_COLUMN_SUFFIX,
                new WrappedLongArraySource(() -> nanCount));
        }
        if (positiveInfinityCount != null) {
            results.put(name + ROLLUP_PIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, positiveInfinityCount);
        } else {
            results.put(name + ROLLUP_PIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX,
                new WrappedLongArraySource(() -> positiveInfinityCount));
        }
        if (negativeInfinityCount != null) {
            results.put(name + ROLLUP_NIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX, negativeInfinityCount);
        } else {
            results.put(name + ROLLUP_NIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX,
                new WrappedLongArraySource(() -> negativeInfinityCount));
        }
        return results;
    }

    private static class WrappedLongArraySource extends AbstractColumnSource<Long>
        implements MutableColumnSourceGetDefaults.ForLong {
        final Supplier<LongArraySource> sourceSupplier;

        private WrappedLongArraySource(Supplier<LongArraySource> sourceSupplier) {
            super(long.class);
            this.sourceSupplier = sourceSupplier;
        }

        @Override
        public long getLong(long index) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return QueryConstants.NULL_LONG;
            } else {
                return longArraySource.getLong(index);
            }
        }

        @Override
        public long getPrevLong(long index) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return QueryConstants.NULL_LONG;
            } else {
                return longArraySource.getPrevLong(index);
            }
        }

        @Override
        public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return new DefaultGetContext<>(this, chunkCapacity, sharedContext);
            } else {
                return longArraySource.makeGetContext(chunkCapacity, sharedContext);
            }
        }

        @Override
        public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return ChunkSource.DEFAULT_FILL_INSTANCE;
            } else {
                return longArraySource.makeFillContext(chunkCapacity, sharedContext);
            }
        }

        @Override
        public void fillChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Attributes.Values> destination,
            @NotNull OrderedKeys orderedKeys) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                destination.fillWithNullValue(0, orderedKeys.intSize());
            } else {
                longArraySource.fillChunk(context, destination, orderedKeys);
            }
        }

        @Override
        public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Attributes.Values> destination,
            @NotNull OrderedKeys orderedKeys) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                destination.fillWithNullValue(0, orderedKeys.intSize());
            } else {
                longArraySource.fillPrevChunk(context, destination, orderedKeys);
            }
        }

        @Override
        public Chunk<? extends Attributes.Values> getChunk(@NotNull GetContext context,
            @NotNull OrderedKeys orderedKeys) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return doNullFill((DefaultGetContext) context, orderedKeys.intSize());
            } else {
                return longArraySource.getChunk(context, orderedKeys);
            }
        }

        @Override
        public Chunk<? extends Attributes.Values> getChunk(@NotNull GetContext context,
            long firstKey, long lastKey) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return doNullFill((DefaultGetContext) context,
                    LongSizedDataStructure.intSize("getChunk", lastKey - firstKey + 1));
            } else {
                return longArraySource.getChunk(context, firstKey, lastKey);
            }
        }

        @NotNull
        private Chunk<Attributes.Values> doNullFill(@NotNull DefaultGetContext context, int size) {
            // noinspection unchecked
            final WritableChunk<Attributes.Values> resultChunk = context.getWritableChunk();
            resultChunk.fillWithNullValue(0, size);
            resultChunk.setSize(size);
            return resultChunk;
        }

        @Override
        public Chunk<? extends Attributes.Values> getPrevChunk(@NotNull GetContext context,
            @NotNull OrderedKeys orderedKeys) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return doNullFill((DefaultGetContext) context, orderedKeys.intSize());
            } else {
                return longArraySource.getPrevChunk(context, orderedKeys);
            }
        }

        @Override
        public Chunk<? extends Attributes.Values> getPrevChunk(@NotNull GetContext context,
            long firstKey, long lastKey) {
            final LongArraySource longArraySource = sourceSupplier.get();
            if (longArraySource == null) {
                return doNullFill((DefaultGetContext) context,
                    LongSizedDataStructure.intSize("getPrevChunk", lastKey - firstKey + 1));
            } else {
                return longArraySource.getPrevChunk(context, firstKey, lastKey);
            }
        }
    }
}
