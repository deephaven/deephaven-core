/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkHasher and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.chunk.util.hashing.ChunkHasher.scrambleHash;

public class FloatChunkHasher implements ChunkHasher {
    public static FloatChunkHasher INSTANCE = new FloatChunkHasher();

    private static void hashInitial(FloatChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final float value = values.get(ii);
            destination.set(ii, hashInitialSingle(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(FloatChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateSingle(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    public static int hashInitialSingle(float value) {
        return scrambleHash(Float.hashCode(value));
    }

    public static int hashUpdateSingle(int existing, float newValue) {
        return existing * 31 + hashInitialSingle(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialSingle(TypeUtils.unbox((Float)value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateSingle(existing, TypeUtils.unbox((Float)value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashInitial(values.asFloatChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashSecondary(values.asFloatChunk(), destination);
    }
}
