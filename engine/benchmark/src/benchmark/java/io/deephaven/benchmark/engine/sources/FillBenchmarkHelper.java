package io.deephaven.benchmark.engine.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.openjdk.jmh.infra.Blackhole;

public interface FillBenchmarkHelper {

    void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys);

    void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence);

    void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence);

    void release();
}
