package io.deephaven.benchmark.engine.sources;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSequence;
import org.openjdk.jmh.infra.Blackhole;

public interface FillBenchmarkHelper {
    void getFromArray(Blackhole bh, int fetchSize, LongChunk<Attributes.OrderedRowKeys> keys);

    void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence);

    void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence);

    void release();
}
