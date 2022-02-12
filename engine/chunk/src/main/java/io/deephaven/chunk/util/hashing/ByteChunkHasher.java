/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkHasher and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.chunk.util.hashing.ChunkHasher.scrambleHash;

public class ByteChunkHasher implements ChunkHasher {
    public static ByteChunkHasher INSTANCE = new ByteChunkHasher();

    private static void hashInitial(ByteChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final byte value = values.get(ii);
            destination.set(ii, hashInitialSingle(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(ByteChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateSingle(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    public static int hashInitialSingle(byte value) {
        return scrambleHash(Byte.hashCode(value));
    }

    public static int hashUpdateSingle(int existing, byte newValue) {
        return existing * 31 + hashInitialSingle(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialSingle(TypeUtils.unbox((Byte)value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateSingle(existing, TypeUtils.unbox((Byte)value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashInitial(values.asByteChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashSecondary(values.asByteChunk(), destination);
    }
}
