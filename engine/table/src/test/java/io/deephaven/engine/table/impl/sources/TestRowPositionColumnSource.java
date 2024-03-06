//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.QueryConstants;
import org.junit.Rule;
import org.junit.Test;

public class TestRowPositionColumnSource {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testGet() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final long RNG_MULTIPLE = 3;
        final long RNG_SIZE = 8;
        for (long ii = 0; ii < 2048; ++ii) {
            final long firstKey = ii * RNG_MULTIPLE * RNG_SIZE;
            builder.appendRange(firstKey, firstKey + RNG_SIZE - 1);
        }

        try (final TrackingWritableRowSet rs = builder.build().toTracking()) {
            final RowPositionColumnSource source = new RowPositionColumnSource(rs);

            rs.forAllRowKeys(key -> {
                Assert.eq(source.getLong(key), "source.get(key)", rs.find(key), "rs.find(key)");
            });

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            updateGraph.runWithinUnitTestCycle(() -> {
                rs.removeRange(0, rs.get(1023));

                rs.forAllRowKeys(key -> {
                    Assert.eq(source.getPrevLong(key), "source.get(key)", rs.find(key) + 1024, "rs.find(key) - 1024");
                });
            });

            Assert.eq(source.getLong(RowSet.NULL_ROW_KEY), "source.get(NULL_ROW_KEY)",
                    QueryConstants.NULL_LONG, "QueryConstants.NULL_LONG");
            Assert.eq(source.getPrevLong(RowSet.NULL_ROW_KEY), "source.getPrevLong(NULL_ROW_KEY)",
                    QueryConstants.NULL_LONG, "QueryConstants.NULL_LONG");
        }
    }

    @Test
    public void testFillChunk() {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        final long RNG_MULTIPLE = 3;
        final long RNG_SIZE = 8;
        for (long ii = 0; ii < 2048; ++ii) {
            final long firstKey = ii * RNG_MULTIPLE * RNG_SIZE;
            builder.appendRange(firstKey, firstKey + RNG_SIZE - 1);
        }

        try (final TrackingWritableRowSet rs = builder.build().toTracking();
                final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(rs.intSize())) {
            final RowPositionColumnSource source = new RowPositionColumnSource(rs);

            try (final ChunkSource.FillContext context = source.makeFillContext(chunk.capacity())) {
                source.fillChunk(context, chunk, rs);
                for (int ii = 0; ii < chunk.size(); ++ii) {
                    Assert.eq(chunk.get(ii), "chunk.get(ii)", ii, "rs.find(ii)");
                }

                chunk.fillWithNullValue(0, chunk.capacity());
                final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
                updateGraph.runWithinUnitTestCycle(() -> {
                    rs.removeRange(0, rs.get(1023));
                    source.fillPrevChunk(context, chunk, rs);

                    for (int ii = 0; ii < chunk.size(); ++ii) {
                        Assert.eq(chunk.get(ii), "chunk.get(ii)", ii + 1024, "rs.find(ii) - 1024");
                    }
                });
            }
        }
    }
}
