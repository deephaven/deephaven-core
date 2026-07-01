//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-stream registry that maps Arrow dictionary ids to their decoded value chunks.
 *
 * <p>
 * When the barrage/Arrow reader encounters a {@link org.apache.arrow.flatbuf.DictionaryBatch} message, it decodes the
 * single-column body into a {@code WritableChunk<Values>} and calls {@link #update(long, WritableChunk, boolean)} to
 * install or append values. {@link DictionaryChunkReader}s hold a reference to the registry and look up their id during
 * {@code readChunk} to expand index values to their logical type.
 *
 * <p>
 * Thread-safety: not thread-safe; single-threaded stream reading is assumed.
 */
public final class DictionaryReaderRegistry {

    private final Map<Long, List<Object>> dictValues = new HashMap<>();

    /**
     * Installs or updates the dictionary for {@code dictId}.
     *
     * @param dictId the Arrow dictionary id
     * @param valuesChunk the decoded values for this batch; the registry takes logical ownership (copies objects out)
     * @param isDelta {@code false} to replace the whole dictionary; {@code true} to append
     */
    public void update(final long dictId, @NotNull final WritableChunk<Values> valuesChunk, final boolean isDelta) {
        final int n = valuesChunk.size();
        final List<Object> current;
        if (isDelta) {
            current = dictValues.computeIfAbsent(dictId, id -> new ArrayList<>());
        } else {
            current = new ArrayList<>(n);
            dictValues.put(dictId, current);
        }
        for (int i = 0; i < n; ++i) {
            current.add(rawBoxValue(valuesChunk, i));
        }
    }

    /**
     * Returns the ordered list of dictionary values for {@code dictId}, or {@code null} if no dictionary has been
     * received for that id yet.
     */
    @Nullable
    public List<Object> get(final long dictId) {
        return dictValues.get(dictId);
    }

    /**
     * Boxes a single element from a chunk into an Object, preserving primitive sentinels and canonicalizing NaN. Shared
     * with {@link DictionaryChunkWriter#rawBoxValue} semantics; kept here so GWT-compiled readers (which exclude the
     * writer) can call it without a dependency on the writer.
     */
    @Nullable
    static Object rawBoxValue(@NotNull final Chunk<Values> chunk, final int position) {
        switch (chunk.getChunkType()) {
            case Byte:
                return chunk.asByteChunk().get(position);
            case Char:
                return chunk.asCharChunk().get(position);
            case Short:
                return chunk.asShortChunk().get(position);
            case Int:
                return chunk.asIntChunk().get(position);
            case Long:
                return chunk.asLongChunk().get(position);
            case Float: {
                final float v = chunk.asFloatChunk().get(position);
                return Float.isNaN(v) ? Float.NaN : v;
            }
            case Double: {
                final double v = chunk.asDoubleChunk().get(position);
                return Double.isNaN(v) ? Double.NaN : v;
            }
            case Object:
                return chunk.asObjectChunk().get(position);
            default:
                throw new IllegalArgumentException("Unsupported chunk type: " + chunk.getChunkType());
        }
    }
}
