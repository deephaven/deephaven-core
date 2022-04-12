package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.HashCodes;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.chunk.util.hashing.ChunkHasher.scrambleHash;

public class CharChunkHasher implements ChunkHasher {
    public static CharChunkHasher INSTANCE = new CharChunkHasher();

    private static void hashInitial(CharChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final char value = values.get(ii);
            destination.set(ii, hashInitialSingle(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(CharChunk<Values> values, WritableIntChunk<HashCodes> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateSingle(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    public static int hashInitialSingle(char value) {
        return scrambleHash(Character.hashCode(value));
    }

    public static int hashUpdateSingle(int existing, char newValue) {
        return existing * 31 + hashInitialSingle(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialSingle(TypeUtils.unbox((Character)value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateSingle(existing, TypeUtils.unbox((Character)value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashInitial(values.asCharChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCodes> destination) {
        hashSecondary(values.asCharChunk(), destination);
    }
}
