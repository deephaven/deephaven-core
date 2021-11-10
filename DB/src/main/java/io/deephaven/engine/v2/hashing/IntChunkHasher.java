/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkHasher and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.hashing;

import io.deephaven.engine.chunk.IntChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.WritableIntChunk;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.engine.v2.hashing.ChunkHasher.scrambleHash;
import static io.deephaven.engine.chunk.Attributes.*;

public class IntChunkHasher implements ChunkHasher {
    public static IntChunkHasher INSTANCE = new IntChunkHasher();

    private static void hashInitial(IntChunk<Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final int value = values.get(ii);
            destination.set(ii, hashInitialInternal(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(IntChunk<Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateInternal(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    private static int hashInitialInternal(int value) {
        return scrambleHash(Integer.hashCode(value));
    }

    private static int hashUpdateInternal(int existing, int newValue) {
        return existing * 31 + hashInitialInternal(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialInternal(TypeUtils.unbox((Integer)value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateInternal(existing, TypeUtils.unbox((Integer)value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashInitial(values.asIntChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashSecondary(values.asIntChunk(), destination);
    }
}
