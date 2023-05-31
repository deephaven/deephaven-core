/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharTestSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.testutil.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.updategraph.TerminalNotification;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.function.LongConsumer;

/**
 * The ByteTestSource is a ColumnSource used only for testing; not in live code.
 * <p>
 * It uses a fastutil open addressed hash map from long RowSet keys to byte values. Previous data is stored in a
 * completely separate map, which is copied from the primary map on the first change in a given cycle. If an
 * uninitialized key is accessed; then an IllegalStateException is thrown. The previous value map is discarded in an
 * {@link UpdateCommitter} using a {@link TerminalNotification} after the live table monitor cycle is complete.
 */
public class ByteTestSource extends AbstractColumnSource<Byte>
        implements MutableColumnSourceGetDefaults.ForByte, TestColumnSource<Byte> {
    private long lastAdditionTime = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
    protected final Long2ByteOpenHashMap data = new Long2ByteOpenHashMap();
    protected Long2ByteOpenHashMap prevData;

    private final UpdateCommitter<ByteTestSource> prevFlusher =
            new UpdateCommitter<>(this, ByteTestSource::flushPrevious);

    // region empty constructor
    public ByteTestSource() {
        this(RowSetFactory.empty(), ByteChunk.getEmptyChunk());
    }
    // endregion empty constructor

    // region chunk constructor
    public ByteTestSource(RowSet rowSet, Chunk<Values> data) {
        super(byte.class);
        add(rowSet, data);
        setDefaultReturnValue(this.data);
        this.prevData = this.data;
    }
    // endregion chunk constructor

    private void setDefaultReturnValue(Long2ByteOpenHashMap data) {
        data.defaultReturnValue(QueryConstants.NULL_BYTE);
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
        setGroupToRange(null);

        if (rowSet.size() != vs.size()) {
            throw new IllegalArgumentException("Index=" + rowSet + ", data size=" + vs.size());
        }

        maybeInitializePrevForStep();

        if (vs.getChunkType() == ChunkType.Byte) {
            final ByteChunk<Values> vcs = vs.asByteChunk();
            rowSet.forAllRowKeys(new LongConsumer() {
                private final MutableInt ii = new MutableInt(0);

                @Override
                public void accept(final long v) {
                    data.put(v, vcs.get(ii.intValue()));
                    ii.increment();
                }
            });
        } else if (vs.getChunkType() == ChunkType.Object) {
            final ObjectChunk<Byte, Values> vcs = vs.asObjectChunk();
            rowSet.forAllRowKeys(new LongConsumer() {
                private final MutableInt ii = new MutableInt(0);

                @Override
                public void accept(final long v) {
                    data.put(v, TypeUtils.unbox(vcs.get(ii.intValue())));
                    ii.increment();
                }
            });
        } else {
            throw new IllegalArgumentException("Invalid chunk type for " + getClass() + ": " + vs.getChunkType());
        }
    }
    // endregion chunk add

    private void maybeInitializePrevForStep() {
        long currentStep = ExecutionContext.getContext().getUpdateGraph().clock().currentStep();
        if (currentStep == lastAdditionTime) {
            return;
        }
        prevFlusher.maybeActivate();
        prevData = new Long2ByteOpenHashMap(this.data);
        setDefaultReturnValue(prevData);
        lastAdditionTime = currentStep;
    }

    @Override
    public synchronized void remove(RowSet rowSet) {
        setGroupToRange(null);

        maybeInitializePrevForStep();
        rowSet.forAllRowKeys(data::remove);
    }

    @Override
    public synchronized void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        maybeInitializePrevForStep();
        setGroupToRange(null);

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
    public Byte get(long index) {
        return TypeUtils.box(getByte(index));
    }
    // endregion boxed get

    @Override
    public synchronized byte getByte(long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return QueryConstants.NULL_BYTE;
        }
        // If a test asks for a non-existent positive index something is wrong.
        // We have to accept negative values, because e.g. a join may find no matching right key, in which case it
        // has an empty redirection index entry that just gets passed through to the inner column source as -1.
        final byte retVal = data.get(index);
        if (retVal == QueryConstants.NULL_BYTE && !data.containsKey(index)) {
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
    public Byte getPrev(long index) {
        return TypeUtils.box(getPrevByte(index));
    }
    // endregion boxed getPrev

    @Override
    public synchronized byte getPrevByte(long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return QueryConstants.NULL_BYTE;
        }

        if (prevData == null) {
            return getByte(index);
        }

        final byte retVal = prevData.get(index);
        if (retVal == QueryConstants.NULL_BYTE && !prevData.containsKey(index)) {
            throw new IllegalStateException("Asking for a non-existent previous key: " + index);
        }
        return retVal;
    }

    public static void flushPrevious(ByteTestSource source) {
        source.prevData = null;
    }

    @Override
    public void startTrackingPrevValues() {}
}
