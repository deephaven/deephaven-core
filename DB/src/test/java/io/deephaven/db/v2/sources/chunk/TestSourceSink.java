package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.hashing.ChunkEquals;
import io.deephaven.db.v2.sources.WritableChunkSink;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.OrderedKeys;
import junit.framework.TestCase;

import java.util.Random;
import java.util.function.IntFunction;

public class TestSourceSink {
    /**
     * A variety of tests are possible here. As a first pass, we make a chunk of size 1000, fill elements 0-249 and
     * 500-749 with random values, and then see if they come back.
     */
    public static void runTests(ChunkType chunkType, IntFunction<WritableChunkSink> makeSink) {
        final ChunkType containerType = chunkType == ChunkType.Boolean ? ChunkType.Object : chunkType;
        final int chunkSize = 1000; // deliberately not a power of two, for fun.
        final int totalSize = chunkSize * 4;

        WritableChunkSink sink = makeSink.apply(totalSize);

        final RandomResetter randomResetter = RandomResetter.makeRandomResetter(chunkType);
        final Random rng = new Random(120108951);
        final ResettableWritableChunk<Values> chunkA = containerType.makeResettableWritableChunk();
        final ResettableWritableChunk<Values> chunkB = containerType.makeResettableWritableChunk();
        randomResetter.resetWithRandomValues(rng, chunkA, chunkSize);
        randomResetter.resetWithRandomValues(rng, chunkB, chunkSize);

        final OrderedKeys keysA =
                OrderedKeys.wrapKeyRangesChunkAsOrderedKeys(LongChunk.chunkWrap(new long[] {0, chunkSize - 1}));
        final OrderedKeys keysB = OrderedKeys
                .wrapKeyRangesChunkAsOrderedKeys(LongChunk.chunkWrap(new long[] {2 * chunkSize, 3 * chunkSize - 1}));

        final WritableChunkSink.FillFromContext fromContext = sink.makeFillFromContext(chunkSize);
        sink.fillFromChunk(fromContext, chunkA, keysA);
        sink.fillFromChunk(fromContext, chunkB, keysB);

        // Get the whole thing back as one big chunk
        final OrderedKeys keysAll =
                OrderedKeys.wrapKeyRangesChunkAsOrderedKeys(LongChunk.chunkWrap(new long[] {0, 4 * chunkSize - 1}));
        final ChunkSource.GetContext getContext = sink.makeGetContext(totalSize);

        final Chunk<Values> valuesAll = sink.getChunk(getContext, keysAll);

        final WritableChunk<Values> chunkNull = containerType.makeWritableChunk(chunkSize);
        chunkNull.fillWithNullValue(0, chunkSize);

        final ChunkEquals ce = ChunkEquals.makeEqual(containerType);

        equalsHelper("first chunk", ce, chunkA, valuesAll, 0, chunkSize - 1);
        equalsHelper("second chunk", ce, chunkNull, valuesAll, chunkSize, 2 * chunkSize - 1);
        equalsHelper("third chunk", ce, chunkB, valuesAll, 2 * chunkSize, 3 * chunkSize - 1);
        equalsHelper("fourth chunk", ce, chunkNull, valuesAll, 3 * chunkSize, 4 * chunkSize - 1);
    }

    private static void equalsHelper(String what, final ChunkEquals ce, final Chunk<Values> expected,
            final Chunk<Values> actual, final int actualFirst, final int actualLast) {
        final Chunk<Values> actualSlice = actual.slice(actualFirst, actualLast - actualFirst + 1);
        final boolean equals = ce.equalReduce(expected, actualSlice);
        TestCase.assertTrue(what, equals);
    }
}
