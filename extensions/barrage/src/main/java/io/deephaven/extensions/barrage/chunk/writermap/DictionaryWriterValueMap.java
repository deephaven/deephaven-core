//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk.writermap;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An insertion-ordered map from distinct column values to their dictionary indices. One implementation per source
 * {@link ChunkType}; each owns a typed fastutil hash map and a typed fastutil array list so that the hot path (index
 * lookup and registration) and the delta-build path (typed chunk construction) are both free of boxing.
 *
 * <p>
 * Thread-safety: not thread-safe; single-threaded barrage stream serialization is assumed.
 */
public interface DictionaryWriterValueMap {

    /**
     * Fills {@code out} with one dictionary index per logical row in {@code source}/{@code subset}, registering new
     * values as they are encountered. Null rows (in non-deephaven-nulls mode) produce {@code QueryConstants.NULL_INT};
     * non-null rows produce a non-negative dictionary index.
     *
     * <p>
     * {@code out} must be pre-sized to the logical row count by the caller; this method never calls
     * {@code out.setSize()}.
     *
     * @param source the source chunk containing column values
     * @param subset row positions within {@code source} to include; {@code null} means all rows
     * @param useDeephavenNulls when {@code true}, null sentinel values are dictionary-encoded like any other value;
     *        when {@code false}, they map to {@code QueryConstants.NULL_INT}
     * @param out pre-sized output chunk to fill with dictionary indices
     */
    void fillIndexChunk(
            @NotNull Chunk<Values> source,
            @Nullable RowSet subset,
            boolean useDeephavenNulls,
            @NotNull WritableIntChunk<Values> out);

    /**
     * Builds and returns a typed chunk containing the distinct values in insertion order over
     * {@code [fromInclusive, toExclusive)}. The returned chunk is owned by the caller and must be closed when no longer
     * needed.
     */
    @NotNull
    WritableChunk<Values> buildChunk(int fromInclusive, int toExclusive);

    /** Number of distinct values accumulated since construction or the last {@link #reset()}. */
    int size();

    /** Discards all accumulated values and index assignments. */
    void reset();

    static DictionaryWriterValueMap make(final ChunkType valuesChunkType) {
        switch (valuesChunkType) {
            case Byte:
                return new ByteDictionaryWriterValueMap();
            case Char:
                return new CharDictionaryWriterValueMap();
            case Short:
                return new ShortDictionaryWriterValueMap();
            case Int:
                return new IntDictionaryWriterValueMap();
            case Long:
                return new LongDictionaryWriterValueMap();
            case Float:
                return new FloatDictionaryWriterValueMap();
            case Double:
                return new DoubleDictionaryWriterValueMap();
            case Boolean:
                // boolean columns are transported as bytes or boxed objects; fall through to the object map
            case Object:
            default:
                return new ObjectDictionaryWriterValueMap();
        }
    }
}
