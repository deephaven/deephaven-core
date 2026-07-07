//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Serializes a flat column chunk as Arrow Dictionary Encoded on the wire.
 *
 * <p>
 * The RecordBatch for this column contains only the <em>index</em> buffer (an Int16/Int32/Int64 array mapping each row
 * to a position in the dictionary). The dictionary <em>values</em> are shipped separately as
 * {@link org.apache.arrow.flatbuf.DictionaryBatch} messages by
 * {@link io.deephaven.extensions.barrage.BarrageMessageWriterImpl} using the {@link DictionaryWriterState} tracked by
 * this writer.
 *
 * <p>
 * Multiple columns may share the same dictionary id; they will all reference the same {@link DictionaryWriterState}
 * instance (managed by a {@code DictionaryWriterStateManager} held on the stream view). A single
 * {@code DictionaryBatch} is emitted per id per update, covering all new values introduced by any sharing column.
 */
public class DictionaryChunkWriter extends BaseChunkWriter<Chunk<Values>> {
    private static final String DEBUG_NAME = "DictionaryChunkWriter";

    private final long dictId;
    /** Writes the Int8/Int16/Int32/Int64 index column that goes into RecordBatch. */
    private final ChunkWriter<IntChunk<Values>> indexWriter;
    /** Writes the actual column values that go into DictionaryBatch. */
    private final ChunkWriter<Chunk<Values>> valuesWriter;
    private final DictionaryWriterIndexKernel indexKernel;
    private final int indexBitWidth;
    private final ChunkType valuesChunkType;

    public DictionaryChunkWriter(
            final long dictId,
            @NotNull final ChunkWriter<IntChunk<Values>> indexWriter,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            final int indexBitWidth,
            @NotNull final ChunkType valuesChunkType,
            final boolean fieldNullable) {
        super(null, ObjectChunk::getEmptyChunk, 0, false, fieldNullable);
        this.dictId = dictId;
        this.indexWriter = indexWriter;
        this.valuesWriter = valuesWriter;
        this.indexKernel = DictionaryWriterIndexKernel.make(valuesChunkType);
        this.indexBitWidth = indexBitWidth;
        this.valuesChunkType = valuesChunkType;
    }

    public long getDictId() {
        return dictId;
    }

    public ChunkWriter<Chunk<Values>> getValuesWriter() {
        return valuesWriter;
    }

    public ChunkType getValuesChunkType() {
        return valuesChunkType;
    }

    @Override
    protected int computeNullCount(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset) {
        // Not called — we override getInputStream completely.
        throw new UnsupportedOperationException();
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final ChunkWriter.Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        // Not called — we override getInputStream completely.
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Always throws; callers must use {@link #getInputStream(Context, RowSet, BarrageOptions, DictionaryWriterState)}.
     */
    @Override
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        throw new IllegalStateException(
                "DictionaryChunkWriter requires a DictionaryWriterState; call getInputStream(ctx, subset, opts, state)");
    }

    /**
     * Builds the index-column stream for the current batch.
     *
     * <p>
     * Each logical row in {@code subset} is mapped to its 0-based dictionary index using {@code state}. Null rows
     * produce a null-sentinel index value (so the returned stream's validity bitmap has 0 for those positions). Newly
     * encountered non-null values are appended to the dictionary; callers can inspect
     * {@link DictionaryWriterState#hasDelta()} after this call to determine whether a
     * {@link org.apache.arrow.flatbuf.DictionaryBatch} message must be emitted.
     *
     * @param context the chunk context holding the source data
     * @param subset the row offsets (positions within the source chunk) to include; {@code null} means all rows
     * @param options barrage serialization options
     * @param state the per-stream/per-id dictionary state; updated in place with any new values encountered
     */
    public DrainableColumn getInputStream(
            @NotNull final ChunkWriter.Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options,
            @NotNull final DictionaryWriterState state) throws IOException {
        return new DictionaryIndexInputStream(context, subset, options, state);
    }

    /**
     * Builds a chunk containing the {@code deltaValues} (from {@link DictionaryWriterState#getDeltaValues()}) so the
     * caller can serialize them via {@link #valuesWriter} into a {@link org.apache.arrow.flatbuf.DictionaryBatch} body.
     *
     * <p>
     * The returned chunk is owned by the caller and must be closed when done.
     */
    public static WritableChunk<Values> buildDeltaValuesChunk(
            @NotNull final ChunkType valuesChunkType,
            @NotNull final List<Object> deltaValues) {
        final int n = deltaValues.size();
        switch (valuesChunkType) {
            case Byte: {
                final WritableByteChunk<Values> out = WritableByteChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    final Object v = deltaValues.get(i);
                    out.set(i, v == null ? QueryConstants.NULL_BYTE : (Byte) v);
                }
                out.setSize(n);
                return out;
            }
            case Char: {
                final WritableCharChunk<Values> out = WritableCharChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    final Object v = deltaValues.get(i);
                    out.set(i, v == null ? QueryConstants.NULL_CHAR : (Character) v);
                }
                out.setSize(n);
                return out;
            }
            case Short: {
                final WritableShortChunk<Values> out = WritableShortChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    final Object v = deltaValues.get(i);
                    out.set(i, v == null ? QueryConstants.NULL_SHORT : (Short) v);
                }
                out.setSize(n);
                return out;
            }
            case Int: {
                final WritableIntChunk<Values> out = WritableIntChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    final Object v = deltaValues.get(i);
                    out.set(i, v == null ? QueryConstants.NULL_INT : (Integer) v);
                }
                out.setSize(n);
                return out;
            }
            case Long: {
                final WritableLongChunk<Values> out = WritableLongChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    final Object v = deltaValues.get(i);
                    out.set(i, v == null ? QueryConstants.NULL_LONG : (Long) v);
                }
                out.setSize(n);
                return out;
            }
            case Float: {
                final WritableFloatChunk<Values> out = WritableFloatChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    final Object v = deltaValues.get(i);
                    out.set(i, v == null ? QueryConstants.NULL_FLOAT : (Float) v);
                }
                out.setSize(n);
                return out;
            }
            case Double: {
                final WritableDoubleChunk<Values> out = WritableDoubleChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    final Object v = deltaValues.get(i);
                    out.set(i, v == null ? QueryConstants.NULL_DOUBLE : (Double) v);
                }
                out.setSize(n);
                return out;
            }
            case Object: {
                final WritableObjectChunk<Object, Values> out = WritableObjectChunk.makeWritableChunk(n);
                for (int i = 0; i < n; ++i) {
                    out.set(i, deltaValues.get(i));
                }
                out.setSize(n);
                return out;
            }
            default:
                throw new IllegalStateException("Unexpected valuesChunkType: " + valuesChunkType);
        }
    }

    /**
     * Returns the {@link DrainableColumn} for an empty (0-row) dictionary-encoded column batch. The column's validity
     * and index buffers are both empty; the {@link DictionaryWriterState} is registered in the state manager but no new
     * values are added.
     */
    public DrainableColumn getEmptyIndexStream(
            @NotNull final BarrageOptions options) throws IOException {
        return indexWriter.getEmptyInputStream(options);
    }

    // -------------------------------------------------------------------------
    // Private index-column stream implementation
    // -------------------------------------------------------------------------

    private class DictionaryIndexInputStream extends ChunkWriter.DrainableColumn {
        private final ChunkWriter.DrainableColumn indexColumn;

        DictionaryIndexInputStream(
                @NotNull final ChunkWriter.Context context,
                @Nullable final RowSet subset,
                @NotNull final BarrageOptions options,
                @NotNull final DictionaryWriterState state) throws IOException {
            final Chunk<Values> sourceChunk = context.getChunk();
            final int logicalSize = subset == null ? sourceChunk.size() : subset.intSize(DEBUG_NAME);

            // Build the index chunk: one int entry per logical row. In Arrow standard mode null rows
            // produce NULL_INT with a 0-bit in the validity bitmap; in useDeephavenNulls mode the null
            // sentinel is a real dictionary entry and all indices are valid. The indexWriter narrows
            // to the wire bit-width (8/16/32/64) via QueryLanguageFunctionUtils cast functions that
            // preserve the null sentinel through the narrowing.
            final WritableIntChunk<Values> indexChunk =
                    buildIndexChunk(sourceChunk, subset, options, state, logicalSize);
            // getInputStream increments the context ref count; close our own reference so the
            // DrainableColumn becomes the sole owner and frees the chunk when it is closed.
            try (final ChunkWriter.Context idxCtx = indexWriter.makeContext(indexChunk, 0)) {
                this.indexColumn = indexWriter.getInputStream(idxCtx, null, options);
            }
        }

        // --- DrainableColumn delegation ---

        @Override
        public void visitFieldNodes(final ChunkWriter.FieldNodeListener listener) {
            indexColumn.visitFieldNodes(listener);
        }

        @Override
        public void visitBuffers(final ChunkWriter.BufferListener listener) {
            indexColumn.visitBuffers(listener);
        }

        @Override
        public int nullCount() {
            return indexColumn.nullCount();
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            return indexColumn.drainTo(outputStream);
        }

        @Override
        public int available() throws IOException {
            return indexColumn.available();
        }

        @Override
        public void close() throws IOException {
            indexColumn.close();
        }
    }

    // -------------------------------------------------------------------------
    // Index chunk construction
    // -------------------------------------------------------------------------

    private WritableIntChunk<Values> buildIndexChunk(
            @NotNull final Chunk<Values> source,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options,
            @NotNull final DictionaryWriterState state,
            final int logicalSize) {
        final WritableIntChunk<Values> out = WritableIntChunk.makeWritableChunk(logicalSize);
        boolean success = false;
        try {
            out.setSize(logicalSize);
            indexKernel.fillIndexChunk(source, subset, options, state, out);
            final int total = state.totalSize();
            if (indexBitWidth == 8 && total > Byte.MAX_VALUE + 1) {
                throw new IllegalStateException(
                        "Dictionary id=" + dictId + " has exceeded " + (Byte.MAX_VALUE + 1)
                                + " distinct values, which is the maximum for an Int8 index. "
                                + "Switch to a larger index type (e.g. DICTIONARY_ENCODED_INT16 or "
                                + "DICTIONARY_ENCODED_INT32), or reduce the number of distinct values per batch.");
            }
            if (indexBitWidth == 16 && total > Short.MAX_VALUE + 1) {
                throw new IllegalStateException(
                        "Dictionary id=" + dictId + " has exceeded " + (Short.MAX_VALUE + 1)
                                + " distinct values, which is the maximum for an Int16 index. "
                                + "Switch to a larger index type (e.g. DICTIONARY_ENCODED_INT32), "
                                + "or reduce the number of distinct values per batch.");
            }
            success = true;
            return out;
        } finally {
            if (!success) {
                out.close();
            }
        }
    }
}
