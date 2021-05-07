package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.openjdk.jmh.infra.Blackhole;

public interface FillBenchmarkHelper {
    void getFromArray(Blackhole bh, int fetchSize, LongChunk<Attributes.OrderedKeyIndices> keys);

    void fillFromArrayBacked(Blackhole bh, int fetchSize, OrderedKeys orderedKeys);

    void fillFromSparse(Blackhole bh, int fetchSize, OrderedKeys orderedKeys);

    void release();
}
