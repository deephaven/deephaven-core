package io.deephaven.db.v2.hashing;

import io.deephaven.db.v2.sources.chunk.Attributes.HashCode;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;

public interface ChunkHasher {
    /**
     * Called for the first (or only) hash value, sets the hash codes in destination corresponding
     * to values.
     *
     * @param values the values to hash
     * @param destination the chunk to write hash values into
     */
    void hashInitial(Chunk<Values> values, WritableIntChunk<HashCode> destination);

    /**
     * Called for subsequent hash values, updates the hash codes in destination corresponding to
     * values.
     *
     * @param values the values to hash
     * @param destination the chunk to update hash values into
     */
    void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCode> destination);

    /**
     * Hash a boxed object.
     *
     * @param value the boxed object to hash
     * @return the hashcode, as if you called the chunked version of this function
     */
    int hashInitial(Object value);

    /**
     * Update a hash for a boxed object.
     *
     * @param existing the existing hashcode
     * @param value the boxed object to add to the hash code
     * @return the hashcode, as if you called the chunked version of this function
     */
    int hashUpdate(int existing, Object value);

    static ChunkHasher makeHasher(ChunkType chunkType) {
        switch (chunkType) {
            case Boolean:
                return BooleanChunkHasher.INSTANCE;
            case Byte:
                return ByteChunkHasher.INSTANCE;
            case Char:
                return CharChunkHasher.INSTANCE;
            case Int:
                return IntChunkHasher.INSTANCE;
            case Short:
                return ShortChunkHasher.INSTANCE;
            case Long:
                return LongChunkHasher.INSTANCE;
            case Float:
                return FloatChunkHasher.INSTANCE;
            case Double:
                return DoubleChunkHasher.INSTANCE;
            case Object:
                return ObjectChunkHasher.INSTANCE;
        }
        throw new IllegalStateException();
    }

    static int scrambleHash(int x) {
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = (x >> 16) ^ x;
        return x & 0x7fffffff;
    }
}
