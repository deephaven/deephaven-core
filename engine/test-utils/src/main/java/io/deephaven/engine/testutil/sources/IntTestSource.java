//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharTestSource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.function.LongConsumer;

/**
 * The IntTestSource is a ColumnSource used only for testing; not in live code.
 * <p>
 * It uses a fastutil open addressed hash map from long RowSet keys to int values. Previous data is stored in a
 * completely separate map: on the first change in a given cycle, the current map is copied into a fresh map that
 * receives the cycle's mutations, and the prior map is retained as the previous values. A map is never mutated once it
 * has been retained as previous, so readers access the volatile map references without locking while mutators
 * synchronize on this source. If an uninitialized key is accessed; then an IllegalStateException is thrown. The
 * previous value map reference is reset to the current map in an {@link UpdateCommitter} using a
 * {@link TerminalNotification} after the live table monitor cycle is complete.
 */
public class IntTestSource extends AbstractColumnSource<Integer>
        implements MutableColumnSourceGetDefaults.ForInt, TestColumnSource<Integer> {

    private long lastAdditionTime;
    protected volatile Long2IntOpenHashMap data = new Long2IntOpenHashMap();
    protected volatile Long2IntOpenHashMap prevData;

    private final UpdateCommitter<IntTestSource> prevFlusher =
            new UpdateCommitter<>(this, updateGraph, IntTestSource::flushPrevious);

    // region empty constructor
    public IntTestSource() {
        this(RowSetFactory.empty(), IntChunk.getEmptyChunk());
    }
    // endregion empty constructor

    // region chunk constructor
    public IntTestSource(RowSet rowSet, Chunk<Values> data) {
        super(int.class);
        lastAdditionTime = updateGraph.clock().currentStep();
        add(rowSet, data);
        setDefaultReturnValue(this.data);
        this.prevData = this.data;
    }
    // endregion chunk constructor

    private void setDefaultReturnValue(Long2IntOpenHashMap data) {
        data.defaultReturnValue(QueryConstants.NULL_INT);
    }

    public synchronized void checkIndex(RowSet rowSet) {
        Assert.eq(data.size(), "data.size()", rowSet.size(), "rowSet.size()");
        final RowSetBuilderRandom builder = RowSetFactory.builderRandom();
        data.keySet().forEach(builder::addKey);
        final RowSet dataRowSet = builder.build();
        Assert.equals(dataRowSet, "dataRowSet", rowSet, "rowSet");
    }

    // region chunk add
    @Override
    public synchronized void add(final RowSet rowSet, Chunk<Values> vs) {
        if (rowSet.size() != vs.size()) {
            throw new IllegalArgumentException("Index=" + rowSet + ", data size=" + vs.size());
        }

        maybeInitializePrevForStep();

        if (vs.getChunkType() == ChunkType.Int) {
            final IntChunk<Values> vcs = vs.asIntChunk();
            rowSet.forAllRowKeys(new LongConsumer() {
                private final MutableInt ii = new MutableInt(0);

                @Override
                public void accept(final long v) {
                    data.put(v, vcs.get(ii.get()));
                    ii.increment();
                }
            });
        } else if (vs.getChunkType() == ChunkType.Object) {
            final ObjectChunk<Integer, Values> vcs = vs.asObjectChunk();
            rowSet.forAllRowKeys(new LongConsumer() {
                private final MutableInt ii = new MutableInt(0);

                @Override
                public void accept(final long v) {
                    data.put(v, TypeUtils.unbox(vcs.get(ii.get())));
                    ii.increment();
                }
            });
        } else {
            throw new IllegalArgumentException("Invalid chunk type for " + getClass() + ": " + vs.getChunkType());
        }
    }
    // endregion chunk add

    private void maybeInitializePrevForStep() {
        long currentStep = updateGraph.clock().currentStep();
        if (currentStep == lastAdditionTime) {
            return;
        }
        prevFlusher.maybeActivate();
        final Long2IntOpenHashMap newData = new Long2IntOpenHashMap(this.data);
        setDefaultReturnValue(newData);
        prevData = data;
        data = newData;
        lastAdditionTime = currentStep;
    }

    @Override
    public synchronized void remove(RowSet rowSet) {
        maybeInitializePrevForStep();
        rowSet.forAllRowKeys(data::remove);
    }

    @Override
    public synchronized void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        maybeInitializePrevForStep();

        // Note: moving to the right, we need to start with rightmost data first.
        final long dir = shiftDelta > 0 ? -1 : 1;
        final long len = endKeyInclusive - startKeyInclusive + 1;
        for (long offset = dir < 0 ? len - 1 : 0; dir < 0 ? offset >= 0 : offset < len; offset += dir) {
            if (data.containsKey(startKeyInclusive + offset)) {
                data.put(startKeyInclusive + offset + shiftDelta, data.remove(startKeyInclusive + offset));
            }
        }
    }

    // region boxed get
    @Override
    public Integer get(long index) {
        return TypeUtils.box(getInt(index));
    }
    // endregion boxed get

    @Override
    public int getInt(long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return QueryConstants.NULL_INT;
        }
        // If a test asks for a non-existent positive index something is wrong.
        // We have to accept negative values, because e.g. a join may find no matching right key, in which case it
        // has an empty redirection index entry that just gets passed through to the inner column source as -1.
        final Long2IntOpenHashMap data = this.data;
        final int retVal = data.get(index);
        if (retVal == QueryConstants.NULL_INT && !data.containsKey(index)) {
            throw new IllegalStateException("Asking for a non-existent key: " + index);
        }
        return retVal;
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    // region boxed getPrev
    @Override
    public Integer getPrev(long index) {
        return TypeUtils.box(getPrevInt(index));
    }
    // endregion boxed getPrev

    @Override
    public int getPrevInt(long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return QueryConstants.NULL_INT;
        }

        final Long2IntOpenHashMap prevData = this.prevData;
        final int retVal = prevData.get(index);
        if (retVal == QueryConstants.NULL_INT && !prevData.containsKey(index)) {
            throw new IllegalStateException("Asking for a non-existent previous key: " + index);
        }
        return retVal;
    }

    public static void flushPrevious(IntTestSource source) {
        source.prevData = source.data;
    }

    @Override
    public void startTrackingPrevValues() {}
}
