/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkHasher and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.chunk.util.hashing.ChunkHasher.scrambleHash;

public class LongChunkHasher implements ChunkHasher {
    public static LongChunkHasher INSTANCE = new LongChunkHasher();

    private static void hashInitial(LongChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final long value = values.get(ii);
            destination.set(ii, hashInitialSingle(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(LongChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateSingle(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    public static int hashInitialSingle(long value) {
        return scrambleHash(Long.hashCode(value));
    }

    public static int hashUpdateSingle(int existing, long newValue) {
        return existing * 31 + hashInitialSingle(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialSingle(TypeUtils.unbox((Long)value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateSingle(existing, TypeUtils.unbox((Long)value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashInitial(values.asLongChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashSecondary(values.asLongChunk(), destination);
    }
}
