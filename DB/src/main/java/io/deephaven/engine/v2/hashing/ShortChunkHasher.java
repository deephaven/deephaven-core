/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkHasher and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.hashing;

import io.deephaven.engine.structures.chunk.ShortChunk;
import io.deephaven.engine.structures.chunk.Chunk;
import io.deephaven.engine.structures.chunk.WritableIntChunk;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.engine.v2.hashing.ChunkHasher.scrambleHash;
import static io.deephaven.engine.structures.chunk.Attributes.*;

public class ShortChunkHasher implements ChunkHasher {
    public static ShortChunkHasher INSTANCE = new ShortChunkHasher();

    private static void hashInitial(ShortChunk<Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final short value = values.get(ii);
            destination.set(ii, hashInitialInternal(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(ShortChunk<Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateInternal(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    private static int hashInitialInternal(short value) {
        return scrambleHash(Short.hashCode(value));
    }

    private static int hashUpdateInternal(int existing, short newValue) {
        return existing * 31 + hashInitialInternal(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialInternal(TypeUtils.unbox((Short)value));
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateInternal(existing, TypeUtils.unbox((Short)value));
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashInitial(values.asShortChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashSecondary(values.asShortChunk(), destination);
    }
}
