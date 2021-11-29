package io.deephaven.engine.table.impl.ssa;

import io.deephaven.configuration.Configuration;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import java.util.function.LongConsumer;
import java.util.function.Supplier;

public interface SegmentedSortedArray extends LongSizedDataStructure {
    boolean SEGMENTED_SORTED_ARRAY_VALIDATION =
            Configuration.getInstance().getBooleanWithDefault("SegmentedSortedArray.validation", false);

    static SegmentedSortedArray make(ChunkType chunkType, boolean reverse, int nodeSize) {
        return makeFactory(chunkType, reverse, nodeSize).get();
    }

    static Supplier<SegmentedSortedArray> makeFactory(ChunkType chunkType, boolean reverse, int nodeSize) {
        switch (chunkType) {
            case Char:
                return reverse ? () -> new NullAwareCharReverseSegmentedSortedArray(nodeSize)
                        : () -> new NullAwareCharSegmentedSortedArray(nodeSize);
            case Byte:
                return reverse ? () -> new ByteReverseSegmentedSortedArray(nodeSize)
                        : () -> new ByteSegmentedSortedArray(nodeSize);
            case Short:
                return reverse ? () -> new ShortReverseSegmentedSortedArray(nodeSize)
                        : () -> new ShortSegmentedSortedArray(nodeSize);
            case Int:
                return reverse ? () -> new IntReverseSegmentedSortedArray(nodeSize)
                        : () -> new IntSegmentedSortedArray(nodeSize);
            case Long:
                return reverse ? () -> new LongReverseSegmentedSortedArray(nodeSize)
                        : () -> new LongSegmentedSortedArray(nodeSize);
            case Float:
                return reverse ? () -> new FloatReverseSegmentedSortedArray(nodeSize)
                        : () -> new FloatSegmentedSortedArray(nodeSize);
            case Double:
                return reverse ? () -> new DoubleReverseSegmentedSortedArray(nodeSize)
                        : () -> new DoubleSegmentedSortedArray(nodeSize);
            case Object:
                return reverse ? () -> new ObjectReverseSegmentedSortedArray(nodeSize)
                        : () -> new ObjectSegmentedSortedArray(nodeSize);
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Insert new valuesToInsert into this SSA. The valuesToInsert to insert must be sorted.
     * 
     * @param valuesToInsert the valuesToInsert to insert
     * @param indicesToInsert the corresponding indicesToInsert
     */
    void insert(Chunk<? extends Any> valuesToInsert, LongChunk<? extends RowKeys> indicesToInsert);

    /**
     * Remove valuesToRemove from this SSA. The valuesToRemove to remove must be sorted.
     * 
     * @param valuesToRemove the valuesToRemove to remove
     * @param indicesToRemove the corresponding indices
     */
    void remove(Chunk<? extends Any> valuesToRemove, LongChunk<? extends RowKeys> indicesToRemove);

    /**
     * Remove the values and indices referenced in stampChunk and indicesToRemove. Fill priorRedirections with the
     * redirection value immediately preceding the removed value.
     * 
     * @param stampChunk the values to remove
     * @param indicesToRemove the indices (parallel to the values)
     * @param priorRedirections the output prior redirections (parallel to valeus/indices)
     */
    void removeAndGetPrior(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> indicesToRemove,
            WritableLongChunk<? extends RowKeys> priorRedirections);

    <T extends Any> int insertAndGetNextValue(Chunk<T> valuesToInsert, LongChunk<? extends RowKeys> indicesToInsert,
            WritableChunk<T> nextValue);

    void applyShift(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk, long shiftDelta);

    void applyShiftReverse(Chunk<? extends Any> stampChunk, LongChunk<? extends RowKeys> keyChunk,
            long shiftDelta);

    int getNodeSize();

    /**
     * Call the longConsumer for each of the long row keys in this SegmentedSortedArray.
     *
     * @param longConsumer the long consumer to call
     */
    void forAllKeys(LongConsumer longConsumer);

    SsaChecker makeChecker();

    boolean isReversed();

    /**
     * @return the first row key in this SSA, RowSet.NULL_ROW_KEY when empty.
     */
    long getFirst();

    /**
     * @return the last row key in this SSA, RowSet.NULL_ROW_KEY when empty.
     */
    long getLast();
}
