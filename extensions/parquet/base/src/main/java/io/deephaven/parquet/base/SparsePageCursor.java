//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.LongChunk;
import io.deephaven.parquet.base.materializers.IntMaterializer;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A resumable sparse read of one data page, opened by {@link ColumnPageReader#openSparse}. Each read decodes only the
 * requested rows and leaves the cursor after the last one, so a later read of rows at or after {@link #nextRow()}
 * continues from there instead of walking the page again. The cursor owns the page's decompressed bytes, and reuses its
 * buffers across pages. Not thread safe.
 */
public final class SparsePageCursor {

    private static final byte[] EMPTY = new byte[0];

    private byte[] pageBytes = EMPTY;
    private byte[] levelBytes = EMPTY;

    private PageMaterializerFactory factory;
    private ValuesReader valuesReader;
    private KeyIndexReader keyReader;
    private boolean usesDictionary;
    /** {@code null} for a required column. */
    @Nullable
    private RunLengthBitPackingHybridBufferDecoder dlDecoder;
    /** Rows left in the current definition level run. */
    private int runRemaining;
    private boolean runIsNull;
    private long nextRow;

    /**
     * @return The page-relative row that the next read starts from; requested rows must not precede it
     */
    public long nextRow() {
        return nextRow;
    }

    /**
     * @return Whether the page is dictionary encoded, so that {@link #readKeyValues} may be used
     */
    public boolean usesDictionary() {
        return usesDictionary;
    }

    /**
     * Like {@link ColumnPageReader#materialize}, but decode only the requested rows.
     *
     * @param nullValue The value to be stored under the null entries
     * @param rowRanges Sorted, disjoint, inclusive {@code [first, last]} pairs of page-relative rows, none before
     *        {@link #nextRow()}
     * @param rowCount The number of rows in {@code rowRanges}
     * @return An array of {@code rowCount} values, in the order of {@code rowRanges}
     */
    public Object materialize(
            @NotNull final Object nullValue,
            @NotNull final LongChunk<?> rowRanges,
            final int rowCount) throws IOException {
        return read(factory, valuesReader, nullValue, rowRanges, rowCount);
    }

    /**
     * Like {@link ColumnPageReader#readKeyValues}, but decode only the requested rows. See {@link #materialize}.
     *
     * @return An array of {@code rowCount} keys, in the order of {@code rowRanges}
     */
    public int[] readKeyValues(
            final int nullPlaceholder,
            @NotNull final LongChunk<?> rowRanges,
            final int rowCount) throws IOException {
        if (keyReader == null) {
            // Shares its position with valuesReader.
            keyReader = new KeyIndexReader((DictionaryValuesReader) valuesReader);
        }
        return (int[]) read(IntMaterializer.FACTORY, keyReader, nullPlaceholder, rowRanges, rowCount);
    }

    /**
     * Drop the current page, keeping the buffers for reuse.
     */
    public void release() {
        factory = null;
        valuesReader = null;
        keyReader = null;
        dlDecoder = null;
    }

    /**
     * @return A little-endian buffer of {@code size} bytes for the decompressed page, valid until the next open
     */
    ByteBuffer pageBuffer(final int size) {
        if (pageBytes.length < size) {
            pageBytes = new byte[size];
        }
        return ByteBuffer.wrap(pageBytes, 0, size).order(ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * @return A copy of the remaining bytes of {@code levels}, valid until the next open
     */
    ByteBuffer copyLevels(@NotNull final ByteBuffer levels) {
        final int size = levels.remaining();
        if (levelBytes.length < size) {
            levelBytes = new byte[size];
        }
        levels.duplicate().get(levelBytes, 0, size);
        return ByteBuffer.wrap(levelBytes, 0, size);
    }

    /**
     * Position the cursor at the first row of a page whose buffers were filled by {@link #pageBuffer} and
     * {@link #copyLevels}.
     */
    void open(
            @NotNull final PageMaterializerFactory factory,
            @NotNull final ValuesReader valuesReader,
            final boolean usesDictionary,
            @Nullable final RunLengthBitPackingHybridBufferDecoder dlDecoder) {
        this.factory = factory;
        this.valuesReader = valuesReader;
        this.keyReader = null;
        this.usesDictionary = usesDictionary;
        this.dlDecoder = dlDecoder;
        runRemaining = 0;
        nextRow = 0;
    }

    /**
     * Fill a materializer sized to the requested rows. Its indexes are output positions, independent of where
     * {@code dataReader} is in the page, so unrequested values are skipped rather than decoded.
     */
    private Object read(
            final PageMaterializerFactory factory,
            final ValuesReader dataReader,
            final Object nullValue,
            final LongChunk<?> rowRanges,
            final int rowCount) throws IOException {
        Require.neqNull(factory, "factory");
        final PageMaterializer materializer = factory.makeMaterializerWithNulls(dataReader, nullValue, rowCount);
        int outPos = 0;
        for (int ri = 0; ri < rowRanges.size(); ri += 2) {
            final long first = rowRanges.get(ri);
            Require.geq(first, "first", nextRow, "nextRow");
            skip(dataReader, first - nextRow);
            outPos = fill(materializer, outPos, Math.toIntExact(rowRanges.get(ri + 1) - first + 1));
        }
        return materializer.data();
    }

    private void skip(final ValuesReader dataReader, long rows) throws IOException {
        if (dlDecoder == null) {
            if (rows > 0) {
                dataReader.skip(Math.toIntExact(rows));
                nextRow += rows;
            }
            return;
        }
        // Walk the definition levels one run at a time, so that nulls are located without building the whole page's
        // null offsets.
        while (rows > 0) {
            final int count = nextRun(rows);
            if (!runIsNull) {
                dataReader.skip(count);
            }
            rows -= count;
        }
    }

    private int fill(final PageMaterializer materializer, int outPos, final int rows) throws IOException {
        if (dlDecoder == null) {
            materializer.fillValues(outPos, outPos + rows);
            nextRow += rows;
            return outPos + rows;
        }
        long remaining = rows;
        while (remaining > 0) {
            final int count = nextRun(remaining);
            if (runIsNull) {
                materializer.fillNulls(outPos, outPos + count);
            } else {
                materializer.fillValues(outPos, outPos + count);
            }
            outPos += count;
            remaining -= count;
        }
        return outPos;
    }

    /**
     * Consume up to {@code rows} rows of the current definition level run, reading the next run if this one is
     * exhausted.
     */
    private int nextRun(final long rows) throws IOException {
        while (runRemaining == 0) {
            // noinspection DataFlowIssue
            dlDecoder.readNextRange();
            runRemaining = dlDecoder.currentRangeCount();
            runIsNull = dlDecoder.currentValue() == 0;
        }
        final int count = (int) Math.min(rows, runRemaining);
        runRemaining -= count;
        nextRow += count;
        return count;
    }
}
