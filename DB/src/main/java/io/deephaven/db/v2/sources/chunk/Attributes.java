package io.deephaven.db.v2.sources.chunk;

/**
 * Attributes that may apply to a {@link Chunk}.
 */
public class Attributes {
    /**
     * All attributes must extend from Any.
     */
    public interface Any {
    }

    /**
     * The chunk contains individual values.
     */
    public interface Values extends Any {
    }

    /**
     * The chunk contains bytes of objects which need to be decoded.
     */
    public interface EncodedObjects extends Any {
    }

    /**
     * The chunk contains individual index keys or index ranges.
     */
    public interface Keys extends Values {
    }

    /**
     * The chunk contains index keys, which may be ordered or unordered.
     */
    public interface KeyIndices extends Keys {
    }

    /**
     * The chunk contains individual ordered index keys, which must be in strictly ascending order.
     */
    public interface OrderedKeyIndices extends KeyIndices {
    }

    /**
     * The chunk contains index ranges.
     *
     * These are to be represented as pairs of an inclusive start and an inclusive end in even and
     * odd slots, respectively.
     */
    public interface OrderedKeyRanges extends Keys {
    }

    /**
     * The chunk contains index keys, which may be in any order (and contain duplicates).
     */
    public interface UnorderedKeyIndices extends KeyIndices {
    }

    public interface ChunkPositions extends Keys {
    }
    public interface ChunkLengths extends Any {
    }

    public interface DictionaryKeys extends ChunkPositions {
    }
    /**
     * The chunk contains integer hash codes.
     */
    public interface HashCode extends Any {
    }

    /**
     * This chunk contains longs which are encoded StringSets as a bitnask.
     */
    public interface StringSetBitmasks extends Values {
    }
}
