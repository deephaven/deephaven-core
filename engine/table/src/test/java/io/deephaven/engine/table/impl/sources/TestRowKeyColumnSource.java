//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

public class TestRowKeyColumnSource {
    @Test
    public void testGet() {
        final RowKeyColumnSource source = RowKeyColumnSource.INSTANCE;
        try (final RowSet rs = RowSetFactory.fromRange(0, 2048)) {
            rs.forAllRowKeys(key -> {
                Assert.eq(source.getLong(key), "source.get(key)", key, "key");
                Assert.eq(source.getPrevLong(key), "source.getPrevLong(key)", key, "key");
            });
        }

        Assert.eq(source.getLong(RowSet.NULL_ROW_KEY), "source.get(NULL_ROW_KEY)",
                QueryConstants.NULL_LONG, "QueryConstants.NULL_LONG");
        Assert.eq(source.getPrevLong(RowSet.NULL_ROW_KEY), "source.getPrevLong(NULL_ROW_KEY)",
                QueryConstants.NULL_LONG, "QueryConstants.NULL_LONG");
    }

    @Test
    public void testFillChunk() {
        final RowKeyColumnSource source = RowKeyColumnSource.INSTANCE;

        try (final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(2048);
                final ChunkSource.FillContext context = source.makeFillContext(chunk.capacity())) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            final long RNG_MULTIPLE = 3;
            final long RNG_SIZE = 8;
            for (long ii = 0; ii < chunk.capacity() / RNG_SIZE; ++ii) {
                final long firstKey = ii * RNG_MULTIPLE * RNG_SIZE;
                builder.appendRange(firstKey, firstKey + RNG_SIZE - 1);
            }
            try (final RowSet rs = builder.build()) {
                source.fillChunk(context, chunk, rs);
                for (int ii = 0; ii < chunk.size(); ++ii) {
                    Assert.eq(chunk.get(ii), "chunk.get(ii)", rs.get(ii), "rs.get(ii)");
                }

                chunk.fillWithNullValue(0, chunk.capacity());
                source.fillPrevChunk(context, chunk, rs);
                for (int ii = 0; ii < chunk.size(); ++ii) {
                    Assert.eq(chunk.get(ii), "chunk.get(ii)", rs.get(ii), "rs.get(ii)");
                }
            }
        }
    }

    @Test
    public void testFillChunkUnordered() {
        final RowKeyColumnSource source = RowKeyColumnSource.INSTANCE;

        try (final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(2048);
                final WritableLongChunk<RowKeys> keyChunk = WritableLongChunk.makeWritableChunk(2048);
                final ChunkSource.FillContext context = source.makeFillContext(chunk.capacity())) {

            keyChunk.setSize(0);
            for (long ii = 0; ii < keyChunk.capacity(); ++ii) {
                if (ii % 128 == 0) {
                    keyChunk.add(RowSet.NULL_ROW_KEY);
                } else {
                    keyChunk.add((ii * 104729) % 65536);
                }
            }

            source.fillChunkUnordered(context, chunk, keyChunk);
            for (int ii = 0; ii < chunk.size(); ++ii) {
                if (ii % 128 == 0) {
                    Assert.eq(chunk.get(ii), "chunk.get(ii)", QueryConstants.NULL_LONG, "QueryConstants.NULL_LONG");
                } else {
                    Assert.eq(chunk.get(ii), "chunk.get(ii)", keyChunk.get(ii), "keyChunk.get(ii)");
                }
            }

            chunk.fillWithNullValue(0, chunk.capacity());
            source.fillPrevChunkUnordered(context, chunk, keyChunk);
            for (int ii = 0; ii < chunk.size(); ++ii) {
                if (ii % 128 == 0) {
                    Assert.eq(chunk.get(ii), "chunk.get(ii)", QueryConstants.NULL_LONG, "QueryConstants.NULL_LONG");
                } else {
                    Assert.eq(chunk.get(ii), "chunk.get(ii)", keyChunk.get(ii), "keyChunk.get(ii)");
                }
            }
        }
    }
}
