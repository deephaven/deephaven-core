//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.util.BooleanUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the wire bytes {@link BooleanChunkWriter} produces.
 * <p>
 * A boolean column is two bit-packed buffers: a validity bitmap (set bit = non-null, omitted entirely when there are no
 * nulls) followed by the payload (set bit = TRUE). The writer packs the payload with the same
 * {@link BaseChunkWriter.ValidityBuffer} it uses for validity, so an all-TRUE column reaches
 * {@link BaseChunkWriter.ValidityBuffer#bytes()} without ever having appended a "null" — the case that has to be
 * materialized up front rather than refused.
 */
public class BooleanChunkWriterTest {

    private static final BarrageSubscriptionOptions OPTS = BarrageSubscriptionOptions.builder().build();

    /** Not a multiple of 64, so the bitmaps carry a partial trailing word. */
    private static final int NUM_ROWS = 133;

    private static final int BITMAP_BYTES = ((NUM_ROWS + 63) / 64) * 8;

    /** Bit {@code ii} set when {@code predicate} holds; bits past the last element left clear. */
    private static byte[] expectedBitmap(final IntFunction<Boolean> predicate) {
        final byte[] expected = new byte[BITMAP_BYTES];
        for (int ii = 0; ii < NUM_ROWS; ++ii) {
            if (predicate.apply(ii)) {
                expected[ii / 8] |= (byte) (1 << (ii & 7));
            }
        }
        return expected;
    }

    /** Drains a column of {@code values} and returns the bytes, asserting the reported null count along the way. */
    private static byte[] drain(final IntFunction<Boolean> values, final int expectedNullCount) throws IOException {
        final BooleanChunkWriter<ByteChunk<Values>> writer = BooleanChunkWriter.getIdentity(true);
        final WritableByteChunk<Values> chunk = WritableByteChunk.writableChunkWrap(new byte[NUM_ROWS]);
        for (int ii = 0; ii < NUM_ROWS; ++ii) {
            chunk.set(ii, BooleanUtils.booleanAsByte(values.apply(ii)));
        }
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final ChunkWriter.Context context = writer.makeContext(chunk, 0);
                final ChunkWriter.DrainableColumn column = writer.getInputStream(context, null, OPTS)) {
            assertThat(column.nullCount()).as("nullCount").isEqualTo(expectedNullCount);
            column.drainTo(out);
        }
        return out.toByteArray();
    }

    @Test
    public void allTrueOmitsValidityAndPacksEveryBit() throws IOException {
        // No null, so no validity buffer is sent and the payload is the only thing on the wire. This is the case that
        // asks the packer for bytes without ever having appended a "null".
        final byte[] bytes = drain(index -> Boolean.TRUE, 0);
        assertThat(bytes).hasSize(BITMAP_BYTES);
        assertThat(bytes).containsExactly(expectedBitmap(index -> true));
    }

    @Test
    public void allFalseOmitsValidityAndPacksNoBits() throws IOException {
        final byte[] bytes = drain(index -> Boolean.FALSE, 0);
        assertThat(bytes).hasSize(BITMAP_BYTES);
        assertThat(bytes).containsExactly(new byte[BITMAP_BYTES]);
    }

    @Test
    public void allNullSendsAnEmptyValidityAndNoPayloadBits() throws IOException {
        final byte[] bytes = drain(index -> null, NUM_ROWS);
        assertThat(bytes).hasSize(2 * BITMAP_BYTES);
        // validity: every element null, so no bit set; payload: nothing is TRUE
        assertThat(bytes).containsExactly(new byte[2 * BITMAP_BYTES]);
    }

    @Test
    public void mixedValuesPackValidityThenPayload() throws IOException {
        // null every 5th, TRUE on even indices; transitions land either side of the 64-element word boundaries
        final IntFunction<Boolean> values = index -> index % 5 == 0 ? null : (index % 2 == 0);
        int expectedNulls = 0;
        for (int ii = 0; ii < NUM_ROWS; ++ii) {
            if (values.apply(ii) == null) {
                ++expectedNulls;
            }
        }

        final byte[] bytes = drain(values, expectedNulls);
        assertThat(bytes).hasSize(2 * BITMAP_BYTES);

        final byte[] expectedValidity = expectedBitmap(index -> values.apply(index) != null);
        final byte[] expectedPayload = expectedBitmap(index -> values.apply(index) == Boolean.TRUE);
        final byte[] expected = new byte[2 * BITMAP_BYTES];
        System.arraycopy(expectedValidity, 0, expected, 0, BITMAP_BYTES);
        System.arraycopy(expectedPayload, 0, expected, BITMAP_BYTES, BITMAP_BYTES);
        assertThat(bytes).containsExactly(expected);
    }
}
