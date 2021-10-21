package io.deephaven.engine.v2.sources.chunk;

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
     * The chunk contains indices, e.g. positions or keys designating indices in a data structure.
     */
    public interface Indices extends Values {
    }

    /**
     * The chunk contains row keys, which may be ordered or unordered.
     */
    public interface RowKeys extends Indices {
    }

    /**
     * The chunk contains individual ordered row keys, which must be in strictly ascending order.
     */
    public interface OrderedRowKeys extends RowKeys {
    }

    /**
     * The chunk contains row key ranges.
     *
     * These are to be represented as pairs of an inclusive start and an inclusive end in even and odd slots,
     * respectively.
     */
    public interface OrderedRowKeyRanges extends Values {
    }

    /**
     * The chunk contains row keys, which may be in any order (and contain duplicates).
     */
    public interface UnorderedRowKeys extends RowKeys {
    }

    public interface ChunkPositions extends Indices {
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
