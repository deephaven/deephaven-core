//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    // GWT-friendly maps (vs. Long2ObjectOpenHashMap)
    private final Map<Long, DictionaryValues> dictValues = new HashMap<>();

    /**
     * Installs or updates the dictionary for {@code dictId}.
     *
     * @param dictId the Arrow dictionary id
     * @param valuesChunk the decoded values for this batch; the registry takes logical ownership
     * @param isDelta {@code false} to replace the whole dictionary; {@code true} to append
     */
    public void update(final long dictId, @NotNull final WritableChunk<Values> valuesChunk, final boolean isDelta) {
        if (isDelta) {
            dictValues.computeIfAbsent(dictId, id -> new DictionaryValues(valuesChunk.getChunkType()))
                    .append(valuesChunk);
        } else {
            DictionaryValues values = dictValues.get(dictId);
            if (values == null) {
                values = new DictionaryValues(valuesChunk.getChunkType());
                dictValues.put(dictId, values);
            }
            values.replace(valuesChunk);
        }
    }

    /**
     * Returns the ordered dictionary values for {@code dictId}, or {@code null} if no dictionary has been received for
     * that id yet.
     */
    @Nullable
    public DictionaryValues get(final long dictId) {
        return dictValues.get(dictId);
    }
}
