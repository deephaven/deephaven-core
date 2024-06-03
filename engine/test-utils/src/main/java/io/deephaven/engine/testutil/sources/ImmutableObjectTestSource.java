//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ImmutableCharTestSource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.ImmutableColumnSourceGetDefaults;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.util.mutable.MutableInt;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.function.LongConsumer;

/**
 * A test column source that ignores modifications, throws on removals, and adds when requested.
 * <p>
 * It uses a fastutil open addressed hash map from long RowSet keys to column values. If an uninitialized key is
 * accessed; then an IllegalStateException is thrown. If the test framework attempts to remove or shift values, then an
 * UnsupportedOperationException is thrown.
 */
public class ImmutableObjectTestSource<T> extends AbstractColumnSource<T>
        implements ImmutableColumnSourceGetDefaults.ForObject<T>, TestColumnSource<T> {
    protected final Long2ObjectOpenHashMap<T> data = new Long2ObjectOpenHashMap<T>();

    // region empty constructor
    public ImmutableObjectTestSource(Class<T> type) {
        this(type, RowSetFactory.empty(), ObjectChunk.getEmptyChunk());
    }

    // endregion empty constructor

    // region chunk constructor
    public ImmutableObjectTestSource(Class<T> type, RowSet rowSet, Chunk<Values> data) {
        super(type);
        add(rowSet, data);
        setDefaultReturnValue(this.data);
    }

    // endregion chunk constructor

    private void setDefaultReturnValue(Long2ObjectOpenHashMap<T> data) {
        data.defaultReturnValue(null);
    }

    public synchronized void checkIndex(RowSet rowSet) {
        Assert.eq(data.size(), "data.size()", rowSet.size(), "rowSet.size()");
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        data.keySet().forEach(builder::appendKey);
        final RowSet dataRowSet = builder.build();
        Assert.equals(dataRowSet, "dataRowSet", rowSet, "rowSet");
    }

    // region chunk add
    public synchronized void add(final RowSet rowSet, Chunk<Values> vs) {
        if (rowSet.size() != vs.size()) {
            throw new IllegalArgumentException("rowSet=" + rowSet + ", data size=" + vs.size());
        }

        final ObjectChunk<T, Values> vcs = vs.asObjectChunk();
        rowSet.forAllRowKeys(new LongConsumer() {
            private final MutableInt ii = new MutableInt(0);

            @Override
            public void accept(final long v) {
                // the unit test framework will ask us to add things, we need to conveniently ignore it
                if (!data.containsKey(v)) {
                    data.put(v, vcs.get(ii.get()));
                }
                ii.increment();
            }
        });
    }
    // endregion chunk add

    public synchronized void remove(RowSet rowSet) {
        throw new IllegalStateException();
    }

    @Override
    public synchronized void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        throw new IllegalStateException();
    }

    // region boxed get
    // endregion boxed get

    @Override
    public synchronized T get(long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return null;
        }
        // If a test asks for a non-existent positive index something is wrong.
        // We have to accept negative values, because e.g. a join may find no matching right key, in which case it
        // has an empty redirection index entry that just gets passed through to the inner column source as -1.
        final T retVal = data.get(index);
        if (retVal == null && !data.containsKey(index)) {
            throw new IllegalStateException("Asking for a non-existent key: " + index);
        }
        return retVal;
    }

    @Override
    public boolean isImmutable() {
        return true;
    }

    @Override
    public void startTrackingPrevValues() {}
}
