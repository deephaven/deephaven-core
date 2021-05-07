/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkHasher and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.hashing;

import java.util.Objects;

import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.util.type.TypeUtils;

import static io.deephaven.db.v2.hashing.ChunkHasher.scrambleHash;
import static io.deephaven.db.v2.sources.chunk.Attributes.*;

public class ObjectChunkHasher implements ChunkHasher {
    public static ObjectChunkHasher INSTANCE = new ObjectChunkHasher();

    private static void hashInitial(ObjectChunk<Object, Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            final Object value = values.get(ii);
            destination.set(ii, hashInitialInternal(value));
        }
        destination.setSize(values.size());
    }

    private static void hashSecondary(ObjectChunk<Object, Values> values, WritableIntChunk<HashCode> destination) {
        for (int ii = 0; ii < values.size(); ++ii) {
            destination.set(ii, hashUpdateInternal(destination.get(ii), values.get(ii)));
        }
        destination.setSize(values.size());
    }

    private static int hashInitialInternal(Object value) {
        return scrambleHash(Objects.hashCode(value));
    }

    private static int hashUpdateInternal(int existing, Object newValue) {
        return existing * 31 + hashInitialInternal(newValue);
    }

    @Override
    public int hashInitial(Object value) {
        return hashInitialInternal(value);
    }

    @Override
    public int hashUpdate(int existing, Object value) {
        return hashUpdateInternal(existing, value);
    }

    @Override
    public void hashInitial(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashInitial(values.asObjectChunk(), destination);
    }

    @Override
    public void hashUpdate(Chunk<Values> values, WritableIntChunk<HashCode> destination) {
        hashSecondary(values.asObjectChunk(), destination);
    }
}
