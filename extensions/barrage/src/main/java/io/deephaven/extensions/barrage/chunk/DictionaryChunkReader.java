//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
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
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * Reads an Arrow Dictionary-Encoded column from the wire into a flat Deephaven chunk.
 *
 * <p>
 * The RecordBatch column contains an integer-index array (Int16/Int32/Int64) mapping each row to a position in the
 * dictionary. The dictionary values were received in earlier {@link org.apache.arrow.flatbuf.DictionaryBatch} messages
 * and are tracked in the {@link DictionaryReaderRegistry}.
 *
 * <p>
 * This reader consumes one {@link ChunkWriter.FieldNodeInfo} (the index column's node) and the corresponding buffers
 * (validity + index values), then expands the indices to the logical value type using the registry.
 */
public class DictionaryChunkReader extends BaseChunkReader<WritableChunk<Values>> {

    private final long dictId;
    private final ChunkReader<? extends WritableChunk<Values>> indexReader;
    private final ChunkType valuesChunkType;
    private final DictionaryReaderRegistry registry;

    public DictionaryChunkReader(
            final long dictId,
            @NotNull final ChunkReader<? extends WritableChunk<Values>> indexReader,
            @NotNull final ChunkType valuesChunkType,
            @NotNull final DictionaryReaderRegistry registry) {
        this.dictId = dictId;
        this.indexReader = indexReader;
        this.valuesChunkType = valuesChunkType;
        this.registry = registry;
    }

    @Override
    public WritableChunk<Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {

        // Read the raw index column (validity + index values).
        try (final WritableChunk<Values> rawIndices =
                indexReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {

            final int numRows = rawIndices.size();
            final WritableChunk<Values> out = BaseChunkReader.castOrCreateChunk(
                    outChunk, outOffset, Math.max(totalRows, numRows),
                    valuesChunkType::makeWritableChunk,
                    c -> c);

            if (numRows == 0) {
                return out;
            }

            final DictionaryValues dict = registry.get(dictId);
            if (dict == null) {
                throw new IOException("No DictionaryBatch received for dictionary id " + dictId
                        + " before RecordBatch that references it");
            }

            final WritableIntChunk<Values> intIndices = maybeCastToInt(rawIndices);
            try {
                expandIndices(intIndices, dict, out, outOffset, valuesChunkType);
            } finally {
                if (intIndices != rawIndices) {
                    intIndices.close();
                }
            }

            return out;
        }
    }

    // -------------------------------------------------------------------------
    // Index normalization
    // -------------------------------------------------------------------------

    /**
     * Returns an {@link WritableIntChunk} view of {@code indices}. For {@code Int} chunks, returns the chunk itself (no
     * copy). For {@code Short} and {@code Long} chunks, allocates a new {@code WritableIntChunk}; the caller must close
     * it when it differs from the argument.
     */
    private static WritableIntChunk<Values> maybeCastToInt(final WritableChunk<Values> indices) {
        switch (indices.getChunkType()) {
            case Byte: {
                final var src = indices.asByteChunk();
                final int n = src.size();
                final WritableIntChunk<Values> dst = WritableIntChunk.makeWritableChunk(n);
                dst.setSize(n);
                for (int i = 0; i < n; ++i) {
                    final byte b = src.get(i);
                    dst.set(i, b == QueryConstants.NULL_BYTE ? QueryConstants.NULL_INT : (int) b);
                }
                return dst;
            }
            case Short: {
                final var src = indices.asShortChunk();
                final int n = src.size();
                final WritableIntChunk<Values> dst = WritableIntChunk.makeWritableChunk(n);
                dst.setSize(n);
                for (int i = 0; i < n; ++i) {
                    final short s = src.get(i);
                    dst.set(i, s == QueryConstants.NULL_SHORT ? QueryConstants.NULL_INT : (int) s);
                }
                return dst;
            }
            case Int:
                return indices.asWritableIntChunk();
            case Long: {
                final var src = indices.asLongChunk();
                final int n = src.size();
                final WritableIntChunk<Values> dst = WritableIntChunk.makeWritableChunk(n);
                dst.setSize(n);
                for (int i = 0; i < n; ++i) {
                    final long l = src.get(i);
                    dst.set(i, l == QueryConstants.NULL_LONG ? QueryConstants.NULL_INT : (int) l);
                }
                return dst;
            }
            default:
                throw new IllegalStateException(
                        "Dictionary index ChunkType must be Short, Int, or Long; got: " + indices.getChunkType());
        }
    }

    // -------------------------------------------------------------------------
    // Index expansion to values
    // -------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void expandIndices(
            @NotNull final WritableIntChunk<Values> indices,
            @NotNull final DictionaryValues dict,
            @NotNull final WritableChunk<Values> out,
            final int outOffset,
            @NotNull final ChunkType valuesChunkType) {
        final int n = indices.size();
        switch (valuesChunkType) {
            case Byte: {
                final WritableByteChunk<Values> typedOut = out.asWritableByteChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i,
                            idx == QueryConstants.NULL_INT ? QueryConstants.NULL_BYTE : dict.getByte(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            case Char: {
                final WritableCharChunk<Values> typedOut = out.asWritableCharChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i,
                            idx == QueryConstants.NULL_INT ? QueryConstants.NULL_CHAR : dict.getChar(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            case Short: {
                final WritableShortChunk<Values> typedOut = out.asWritableShortChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i,
                            idx == QueryConstants.NULL_INT ? QueryConstants.NULL_SHORT : dict.getShort(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            case Int: {
                final WritableIntChunk<Values> typedOut = out.asWritableIntChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i,
                            idx == QueryConstants.NULL_INT ? QueryConstants.NULL_INT : dict.getInt(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            case Long: {
                final WritableLongChunk<Values> typedOut = out.asWritableLongChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i,
                            idx == QueryConstants.NULL_INT ? QueryConstants.NULL_LONG : dict.getLong(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            case Float: {
                final WritableFloatChunk<Values> typedOut = out.asWritableFloatChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i,
                            idx == QueryConstants.NULL_INT ? QueryConstants.NULL_FLOAT : dict.getFloat(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            case Double: {
                final WritableDoubleChunk<Values> typedOut = out.asWritableDoubleChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i,
                            idx == QueryConstants.NULL_INT ? QueryConstants.NULL_DOUBLE : dict.getDouble(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            case Object: {
                final WritableObjectChunk<Object, Values> typedOut = out.asWritableObjectChunk();
                for (int i = 0; i < n; ++i) {
                    final int idx = indices.get(i);
                    typedOut.set(outOffset + i, idx == QueryConstants.NULL_INT ? null : dict.getObject(idx));
                }
                typedOut.setSize(outOffset + n);
                break;
            }
            default:
                throw new IllegalStateException("Unexpected valuesChunkType: " + valuesChunkType);
        }
    }
}
