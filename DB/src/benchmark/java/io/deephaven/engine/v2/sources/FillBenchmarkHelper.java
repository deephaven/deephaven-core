package io.deephaven.engine.v2.sources;

import io.deephaven.engine.v2.sources.chunk.Attributes;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.structures.RowSequence;
import org.openjdk.jmh.infra.Blackhole;

public interface FillBenchmarkHelper {
    void getFromArray(Blackhole bh, int fetchSize, LongChunk<Attributes.OrderedRowKeys> keys);

    void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence);

    void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence);

    void release();
}
