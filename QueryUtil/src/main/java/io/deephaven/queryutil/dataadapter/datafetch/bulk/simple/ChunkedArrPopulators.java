package io.deephaven.queryutil.dataadapter.datafetch.bulk.simple;

import io.deephaven.queryutil.dataadapter.ChunkToArrayUtil;

/**
 * Created by rbasralian on 3/7/22
 */
@Deprecated
public final class ChunkedArrPopulators {

    public static final ChunkedArrPopulator<Byte, byte[]> BYTE_ARR_POPULATOR = ChunkToArrayUtil::populateArrFromChunk;
    public static final ChunkedArrPopulator<Character, char[]> CHAR_ARR_POPULATOR = ChunkToArrayUtil::populateArrFromChunk;
    public static final ChunkedArrPopulator<Short, short[]> SHORT_ARR_POPULATOR = ChunkToArrayUtil::populateArrFromChunk;
    public static final ChunkedArrPopulator<Integer, int[]> INT_ARR_POPULATOR = ChunkToArrayUtil::populateArrFromChunk;
    public static final ChunkedArrPopulator<Float, float[]> FLOAT_ARR_POPULATOR = ChunkToArrayUtil::populateArrFromChunk;
    public static final ChunkedArrPopulator<Long, long[]> LONG_ARR_POPULATOR = ChunkToArrayUtil::populateArrFromChunk;
    public static final ChunkedArrPopulator<Double, double[]> DOUBLE_ARR_POPULATOR = ChunkToArrayUtil::populateArrFromChunk;

    private ChunkedArrPopulators() {
    }

    public static <T> ChunkedArrPopulator<T, T[]> getObjChunkPopulator() {
        return ChunkToArrayUtil::populateObjArrFromChunk;
    }
}
