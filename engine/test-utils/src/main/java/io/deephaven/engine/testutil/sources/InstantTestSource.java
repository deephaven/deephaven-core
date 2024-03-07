//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;

/**
 * Instant column source that wraps and delegates the storage to an {@code TestSource<Long>}. This also provides an
 * interface so this column can be interpreted as a long column (through UnboxedInstantTestSource).
 */
public class InstantTestSource extends AbstractColumnSource<Instant>
        implements MutableColumnSourceGetDefaults.ForObject<Instant>, TestColumnSource<Instant> {

    private final LongTestSource longTestSource;
    private final UnboxedInstantTestSource alternateColumnSource;

    /**
     * Create a new InstantTestSource with no initial data.
     */
    public InstantTestSource() {
        super(Instant.class);
        this.longTestSource = new LongTestSource();
        this.alternateColumnSource = new UnboxedInstantTestSource(this, longTestSource);
    }

    /**
     * Create a new InstantTestSource with the given rowSet and data.
     *
     * @param rowSet The row keys for the initial data
     * @param data The initial data
     */
    public InstantTestSource(RowSet rowSet, Instant[] data) {
        super(Instant.class);
        this.longTestSource = new LongTestSource(rowSet, mapData(data));
        this.alternateColumnSource = new UnboxedInstantTestSource(this, longTestSource);
    }

    /**
     * Create a new InstantTestSource with the given rowSet and data.
     *
     * @param rowSet The row keys for the initial data
     * @param data The initial data
     */
    public InstantTestSource(RowSet rowSet, Chunk<Values> data) {
        super(Instant.class);
        if (data.getChunkType() == ChunkType.Long) {
            this.longTestSource = new LongTestSource(rowSet, data.asLongChunk());
        } else {
            this.longTestSource = new LongTestSource(rowSet, mapData(data.asObjectChunk()));
        }

        this.alternateColumnSource = new UnboxedInstantTestSource(this, longTestSource);
    }

    private LongChunk<Values> mapData(Instant[] data) {
        final long[] result = new long[data.length];
        for (int ii = 0; ii < result.length; ++ii) {
            final Instant dt = data[ii];
            result[ii] = dt == null ? QueryConstants.NULL_LONG : DateTimeUtils.epochNanos(dt);
        }
        return LongChunk.chunkWrap(result);
    }

    private LongChunk<Values> mapData(ObjectChunk<?, Values> data) {
        final long[] result = new long[data.size()];
        if (result.length > 0 && data.get(0) instanceof Long) {
            final ObjectChunk<Long, Values> boxedLongChunk = data.asObjectChunk();
            for (int ii = 0; ii < result.length; ++ii) {
                result[ii] = TypeUtils.unbox(boxedLongChunk.get(ii));
            }
        } else {
            final ObjectChunk<Instant, Values> dtc = data.asObjectChunk();
            for (int ii = 0; ii < result.length; ++ii) {
                final Instant dt = dtc.get(ii);
                result[ii] = dt == null ? QueryConstants.NULL_LONG : DateTimeUtils.epochNanos(dt);
            }
        }
        return LongChunk.chunkWrap(result);
    }

    public void add(RowSet rowSet, Instant[] data) {
        longTestSource.add(rowSet, mapData(data));
    }

    @Override
    public void add(RowSet rowSet, Chunk<Values> data) {
        if (data.getChunkType() == ChunkType.Long) {
            longTestSource.add(rowSet, data.asLongChunk());
        } else if (data.getChunkType() == ChunkType.Object) {
            longTestSource.add(rowSet, mapData(data.asObjectChunk()));
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void remove(RowSet rowSet) {
        longTestSource.remove(rowSet);
    }

    @Override
    public void shift(long startKeyInclusive, long endKeyInclusive, long shiftDelta) {
        longTestSource.shift(startKeyInclusive, endKeyInclusive, shiftDelta);
    }

    @Override
    public Instant get(long rowKey) {
        final Long v = longTestSource.get(rowKey);
        return v == null ? null : DateTimeUtils.epochNanosToInstant(v);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    @Override
    public long getLong(long rowKey) {
        return longTestSource.getLong(rowKey);
    }

    @Override
    public Instant getPrev(long rowKey) {
        final Long v = longTestSource.getPrev(rowKey);
        return v == null ? null : DateTimeUtils.epochNanosToInstant(v);
    }

    @Override
    public long getPrevLong(long rowKey) {
        return longTestSource.getPrevLong(rowKey);
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class;
    }

    @Override
    public <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) throws IllegalArgumentException {
        // noinspection unchecked
        return (ColumnSource<ALTERNATE_DATA_TYPE>) alternateColumnSource;
    }
}
