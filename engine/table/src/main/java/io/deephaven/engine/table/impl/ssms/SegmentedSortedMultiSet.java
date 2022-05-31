package io.deephaven.engine.table.impl.ssms;

import io.deephaven.configuration.Configuration;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.function.Supplier;

/**
 * MultiSet of primitive or object values stored as parallel arrays of counts and values. Nulls disallowed.
 *
 * @param <T>
 */
public interface SegmentedSortedMultiSet<T> extends LongSizedDataStructure {
    boolean SEGMENTED_SORTED_MULTISET_VALIDATION =
            Configuration.getInstance().getBooleanWithDefault("SegmentedSortedMultiSet.validation", false);

    static SegmentedSortedMultiSet make(ChunkType chunkType, int nodeSize, Class<?> objectType) {
        return makeFactory(chunkType, nodeSize, objectType).get();
    }

    static Supplier<SegmentedSortedMultiSet> makeFactory(ChunkType chunkType, int nodeSize, Class<?> objectType) {
        switch (chunkType) {
            case Char:
                return () -> new CharSegmentedSortedMultiset(nodeSize);
            case Byte:
                return () -> new ByteSegmentedSortedMultiset(nodeSize);
            case Short:
                return () -> new ShortSegmentedSortedMultiset(nodeSize);
            case Int:
                return () -> new IntSegmentedSortedMultiset(nodeSize);
            case Long:
                return () -> new LongSegmentedSortedMultiset(nodeSize);
            case Float:
                return () -> new FloatSegmentedSortedMultiset(nodeSize);
            case Double:
                return () -> new DoubleSegmentedSortedMultiset(nodeSize);
            case Object:
                return () -> new ObjectSegmentedSortedMultiset(nodeSize, objectType);
            default:
            case Boolean:
                throw new UnsupportedOperationException();
        }
    }

    static Supplier<RemoveContext> makeRemoveContextFactory(int nodeSize) {
        return () -> makeRemoveContext(nodeSize);
    }

    static RemoveContext makeRemoveContext(int nodeSize) {
        return new RemoveContext(nodeSize);
    }

    /**
     * Insert new valuesToInsert into this SSMS. The valuesToInsert to insert must be sorted, without duplicates.
     *
     * The valuesToInsert and counts chunks will be modified during this call, and the resulting chunks are undefined.
     *
     * @param valuesToInsert the valuesToInsert to insert
     * @param counts the number of times each value occurs
     * @return true if any new values were inserted
     */
    boolean insert(WritableChunk<? extends Values> valuesToInsert, WritableIntChunk<ChunkLengths> counts);

    /**
     * Remove valuesToRemove from this SSMS. The valuesToRemove to remove must be sorted.
     *
     * @param removeContext removalContext
     * @param valuesToRemove the valuesToRemove to remove
     * @return true if any values were removed.
     */
    boolean remove(RemoveContext removeContext, WritableChunk<? extends Values> valuesToRemove,
            WritableIntChunk<ChunkLengths> lengths);

    @NotNull
    @VisibleForTesting
    WritableChunk<?> keyChunk();

    default void fillKeyChunk(WritableChunk<?> keyChunk, int offset) {}

    @NotNull
    @VisibleForTesting
    WritableLongChunk<?> countChunk();

    class RemoveContext {
        RemoveContext(int leafSize) {
            this.compactionLocations = new int[leafSize];
            this.compactionLengths = new int[leafSize];
        }

        final int[] compactionLocations;
        final int[] compactionLengths;

        int[] compactionLeafs;
        int[] compactionLeafLengths;

        void ensureLeafCount(int leafCount) {
            if (compactionLeafs == null) {
                compactionLeafs = new int[leafCount];
                compactionLeafLengths = new int[leafCount];
            } else if (compactionLeafs.length < leafCount) {
                compactionLeafs = Arrays.copyOf(compactionLeafs, leafCount);
                compactionLeafLengths = Arrays.copyOf(compactionLeafLengths, leafCount);
            }
        }
    }

    int getNodeSize();

    /**
     * @return the size of the set (i.e. the number of unique elements).
     */
    @Override
    long size();

    /**
     * @return the total size of the set in elements (i.e. if A exists twice, 2 is returned not one)
     */
    long totalSize();

    /**
     * Remove count elements from the front of this SSM and add them to the back of the destination SSM.
     * <p>
     * The minimum element of this SSM must be greater than or equal to the maximum of destination.
     * 
     * @param destination the SegmentedSortedMultiSet to append count elements to
     * @param count how many elements to move to the destination
     */
    void moveFrontToBack(SegmentedSortedMultiSet destination, long count);

    /**
     * Remove count elements from the back of this SSM and add them to the front of the destination SSM.
     * <p>
     * The minimum element of this SSM must be less than or equal to the maximum of destination.
     * 
     * @param destination the SegmentedSortedMultiSet to prepend count elements to
     * @param count how many elements to move to the destination
     */
    void moveBackToFront(SegmentedSortedMultiSet destination, long count);

    /**
     * @return the number of times the minimum value exists in this SSM.
     */
    long getMinCount();

    /**
     * @return the number of times the maximum value exists in this SSM.
     */
    long getMaxCount();

    T getMin();

    T getMax();


    void setTrackDeltas(boolean shouldTrackDeltas);

    void clearDeltas();

    int getAddedSize();

    int getRemovedSize();
}
