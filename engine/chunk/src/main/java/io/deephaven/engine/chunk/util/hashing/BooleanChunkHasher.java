/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkHasher and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.chunk.util.hashing;

import io.deephaven.engine.chunk.BooleanChunk;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.WritableIntChunk;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.engine.chunk.util.hashing.ChunkHasher.scrambleHash;
import static io.deephaven.engine.chunk.Attributes.*;

public class BooleanChunkHasher implements ChunkHasher {
    public static BooleanChunkHasher INSTANCE = new BooleanChunkHasher();

    private static void hashInitial(BooleanChunk<Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final boolean value = values.get(ii);
            destination.set(ii, hashInitialInternal(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(BooleanChunk<Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateInternal(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    private static int hashInitialInternal(boolean value) {
        return scrambleHash(Boolean.hashCode(value));
    }

    private static int hashUpdateInternal(int existing, boolean newValue) {
        return existing * 31 + hashInitialInternal(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialInternal(TypeUtils.unbox((Boolean)value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateInternal(existing, TypeUtils.unbox((Boolean)value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashInitial(values.asBooleanChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashSecondary(values.asBooleanChunk(), destination);
    }
}
