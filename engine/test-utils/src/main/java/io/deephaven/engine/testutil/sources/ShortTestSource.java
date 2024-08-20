//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharTestSource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ShortChunk;
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
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;

import java.util.function.LongConsumer;

/**
 * The ShortTestSource is a ColumnSource used only for testing; not in live code.
 * <p>
 * It uses a fastutil open addressed hash map from long RowSet keys to short values. Previous data is stored in a
 * completely separate map, which is copied from the primary map on the first change in a given cycle. If an
 * uninitialized key is accessed; then an IllegalStateException is thrown. The previous value map is discarded in an
 * {@link UpdateCommitter} using a {@link TerminalNotification} after the live table monitor cycle is complete.
 */
public class ShortTestSource extends AbstractColumnSource<Short>
        implements MutableColumnSourceGetDefaults.ForShort, TestColumnSource<Short> {

    private long lastAdditionTime;
    protected final Long2ShortOpenHashMap data = new Long2ShortOpenHashMap();
    protected Long2ShortOpenHashMap prevData;

    private final UpdateCommitter<ShortTestSource> prevFlusher =
            new UpdateCommitter<>(this, updateGraph, ShortTestSource::flushPrevious);

    // region empty constructor
    public ShortTestSource() {
        this(RowSetFactory.empty(), ShortChunk.getEmptyChunk());
    }
    // endregion empty constructor

    // region chunk constructor
    public ShortTestSource(RowSet rowSet, Chunk<Values> data) {
        super(short.class);
        lastAdditionTime = updateGraph.clock().currentStep();
        add(rowSet, data);
        setDefaultReturnValue(this.data);
        this.prevData = this.data;
    }
    // endregion chunk constructor

    private void setDefaultReturnValue(Long2ShortOpenHashMap data) {
        data.defaultReturnValue(QueryConstants.NULL_SHORT);
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

        if (vs.getChunkType() == ChunkType.Short) {
            final ShortChunk<Values> vcs = vs.asShortChunk();
            rowSet.forAllRowKeys(new LongConsumer() {
                private final MutableInt ii = new MutableInt(0);

                @Override
                public void accept(final long v) {
                    data.put(v, vcs.get(ii.get()));
                    ii.increment();
                }
            });
        } else if (vs.getChunkType() == ChunkType.Object) {
            final ObjectChunk<Short, Values> vcs = vs.asObjectChunk();
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
        prevData = new Long2ShortOpenHashMap(this.data);
        setDefaultReturnValue(prevData);
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
    public Short get(long index) {
        return TypeUtils.box(getShort(index));
    }
    // endregion boxed get

    @Override
    public synchronized short getShort(long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return QueryConstants.NULL_SHORT;
        }
        // If a test asks for a non-existent positive index something is wrong.
        // We have to accept negative values, because e.g. a join may find no matching right key, in which case it
        // has an empty redirection index entry that just gets passed through to the inner column source as -1.
        final short retVal = data.get(index);
        if (retVal == QueryConstants.NULL_SHORT && !data.containsKey(index)) {
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
    public Short getPrev(long index) {
        return TypeUtils.box(getPrevShort(index));
    }
    // endregion boxed getPrev

    @Override
    public synchronized short getPrevShort(long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return QueryConstants.NULL_SHORT;
        }

        if (prevData == null) {
            return getShort(index);
        }

        final short retVal = prevData.get(index);
        if (retVal == QueryConstants.NULL_SHORT && !prevData.containsKey(index)) {
            throw new IllegalStateException("Asking for a non-existent previous key: " + index);
        }
        return retVal;
    }

    public static void flushPrevious(ShortTestSource source) {
        source.prevData = null;
    }

    @Override
    public void startTrackingPrevValues() {}
}
