package io.deephaven.db.v2.sources.chunk;

/**
 * Attributes that may apply to a {@link Chunk}.
 *
 * @IncludeAll
 */
public class Attributes {
    /**
     * All attributes must extend from Any.
     *
     * @IncludeAll
     */
    public interface Any {}

    /**
     * The chunk contains individual values.
     *
     * @IncludeAll
     */
    public interface Values extends Any {}

    /**
     * The chunk contains bytes of objects which need to be decoded.
     *
     * @IncludeAll
     */
    public interface EncodedObjects extends Any {}

    /**
     * The chunk contains individual index keys or index ranges.
     *
     * @IncludeAll
     */
    public interface Keys extends Values {}

    /**
     * The chunk contains index keys, which may be ordered or unordered.
     *
     * @IncludeAll
     */
    public interface KeyIndices extends Keys {}

    /**
     * The chunk contains individual ordered index keys, which must be in strictly ascending order.
     *
     * @IncludeAll
     */
    public interface OrderedKeyIndices extends KeyIndices {}

    /**
     * The chunk contains index ranges.
     *
     * These are to be represented as pairs of an inclusive start and an inclusive end in even and odd
     * slots, respectively.
     *
     * @IncludeAll
     */
    public interface OrderedKeyRanges extends Keys {}

    /**
     * The chunk contains index keys, which may be in any order (and contain duplicates).
     *
     * @IncludeAll
     */
    public interface UnorderedKeyIndices extends KeyIndices {}

    public interface ChunkPositions extends Keys {}
    public interface ChunkLengths extends Any {}

    public interface DictionaryKeys extends ChunkPositions {}
    /**
     * The chunk contains integer hash codes.
     *
     * @IncludeAll
     */
    public interface HashCode extends Any {}

    /**
     * This chunk contains longs which are encoded StringSets as a bitnask.
     */
    public interface StringSetBitmasks extends Values {}
}
