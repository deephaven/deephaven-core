//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.QueryConstants;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryChunkWriterTest {

    // -------------------------------------------------------------------------
    // boxValue - Float
    // -------------------------------------------------------------------------

    @Test
    public void testBoxFloatNullReturnsNull() {
        final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(1);
        chunk.set(0, QueryConstants.NULL_FLOAT);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isNull();
    }

    @Test
    public void testBoxFloatCanonicalNanReturnsFloatNaN() {
        final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(1);
        chunk.set(0, Float.NaN);
        final Object result = DictionaryChunkWriter.boxValue(chunk, 0);
        assertThat(result).isInstanceOf(Float.class);
        assertThat(Float.isNaN((Float) result)).isTrue();
        assertThat(Float.floatToRawIntBits((Float) result))
                .isEqualTo(Float.floatToRawIntBits(Float.NaN));
    }

    @Test
    public void testBoxFloatNonCanonicalNanIsCanonicalizedToFloatNaN() {
        // 0x7f800001 is a signalling NaN with a different bit pattern than Float.NaN (0x7fc00000).
        final float nonCanonicalNaN = Float.intBitsToFloat(0x7f800001);
        assertThat(Float.isNaN(nonCanonicalNaN)).isTrue();

        final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(1);
        chunk.set(0, nonCanonicalNaN);
        final Object result = DictionaryChunkWriter.boxValue(chunk, 0);
        assertThat(result).isInstanceOf(Float.class);
        assertThat(Float.floatToRawIntBits((Float) result))
                .isEqualTo(Float.floatToRawIntBits(Float.NaN));
    }

    @Test
    public void testBoxFloatAllNaNVariantsMapToSameDictionaryIndex() {
        final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(3);
        chunk.set(0, Float.NaN);
        chunk.set(1, Float.intBitsToFloat(0x7f800001)); // signalling NaN
        chunk.set(2, Float.intBitsToFloat(0xffc00000)); // negative quiet NaN
        chunk.setSize(3);

        final DictionaryWriterState state = new LocalDictionaryWriterState(1);
        final Object v0 = DictionaryChunkWriter.boxValue(chunk, 0);
        final Object v1 = DictionaryChunkWriter.boxValue(chunk, 1);
        final Object v2 = DictionaryChunkWriter.boxValue(chunk, 2);

        assertThat(state.indexFor(v0)).isZero();
        assertThat(state.indexFor(v1)).isZero(); // same index — all NaN canonicalize together
        assertThat(state.indexFor(v2)).isZero();
        assertThat(state.getDeltaValues()).hasSize(1);
    }

    @Test
    public void testBoxFloatNormalValueIsPreserved() {
        final WritableFloatChunk<Values> chunk = WritableFloatChunk.makeWritableChunk(1);
        chunk.set(0, 3.14f);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isEqualTo(3.14f);
    }

    // -------------------------------------------------------------------------
    // boxValue - Double
    // -------------------------------------------------------------------------

    @Test
    public void testBoxDoubleNullReturnsNull() {
        final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(1);
        chunk.set(0, QueryConstants.NULL_DOUBLE);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isNull();
    }

    @Test
    public void testBoxDoubleCanonicalNanReturnsDoubleNaN() {
        final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(1);
        chunk.set(0, Double.NaN);
        final Object result = DictionaryChunkWriter.boxValue(chunk, 0);
        assertThat(result).isInstanceOf(Double.class);
        assertThat(Double.isNaN((Double) result)).isTrue();
        assertThat(Double.doubleToRawLongBits((Double) result))
                .isEqualTo(Double.doubleToRawLongBits(Double.NaN));
    }

    @Test
    public void testBoxDoubleNonCanonicalNanIsCanonicalizedToDoubleNaN() {
        // 0x7ff0000000000001L is a signalling NaN with a different bit pattern than Double.NaN.
        final double nonCanonicalNaN = Double.longBitsToDouble(0x7ff0000000000001L);
        assertThat(Double.isNaN(nonCanonicalNaN)).isTrue();

        final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(1);
        chunk.set(0, nonCanonicalNaN);
        final Object result = DictionaryChunkWriter.boxValue(chunk, 0);
        assertThat(result).isInstanceOf(Double.class);
        assertThat(Double.doubleToRawLongBits((Double) result))
                .isEqualTo(Double.doubleToRawLongBits(Double.NaN));
    }

    @Test
    public void testBoxDoubleAllNaNVariantsMapToSameDictionaryIndex() {
        final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(3);
        chunk.set(0, Double.NaN);
        chunk.set(1, Double.longBitsToDouble(0x7ff0000000000001L)); // signalling NaN
        chunk.set(2, Double.longBitsToDouble(0xfff8000000000000L)); // negative quiet NaN
        chunk.setSize(3);

        final DictionaryWriterState state = new LocalDictionaryWriterState(2);
        final Object v0 = DictionaryChunkWriter.boxValue(chunk, 0);
        final Object v1 = DictionaryChunkWriter.boxValue(chunk, 1);
        final Object v2 = DictionaryChunkWriter.boxValue(chunk, 2);

        assertThat(state.indexFor(v0)).isZero();
        assertThat(state.indexFor(v1)).isZero(); // same index — all NaN canonicalize together
        assertThat(state.indexFor(v2)).isZero();
        assertThat(state.getDeltaValues()).hasSize(1);
    }

    @Test
    public void testBoxDoubleNormalValueIsPreserved() {
        final WritableDoubleChunk<Values> chunk = WritableDoubleChunk.makeWritableChunk(1);
        chunk.set(0, 2.71828);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isEqualTo(2.71828);
    }

    // -------------------------------------------------------------------------
    // boxValue - integer primitives (null sentinel -> null)
    // -------------------------------------------------------------------------

    @Test
    public void testBoxIntNullReturnsNull() {
        final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(1);
        chunk.set(0, QueryConstants.NULL_INT);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isNull();
    }

    @Test
    public void testBoxIntNormalValueIsPreserved() {
        final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(1);
        chunk.set(0, 42);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isEqualTo(42);
    }

    @Test
    public void testBoxLongNullReturnsNull() {
        final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(1);
        chunk.set(0, QueryConstants.NULL_LONG);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isNull();
    }

    @Test
    public void testBoxLongNormalValueIsPreserved() {
        final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(1);
        chunk.set(0, 123456789L);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isEqualTo(123456789L);
    }

    @Test
    public void testBoxShortNullReturnsNull() {
        final WritableShortChunk<Values> chunk = WritableShortChunk.makeWritableChunk(1);
        chunk.set(0, QueryConstants.NULL_SHORT);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isNull();
    }

    @Test
    public void testBoxByteNullReturnsNull() {
        final WritableByteChunk<Values> chunk = WritableByteChunk.makeWritableChunk(1);
        chunk.set(0, QueryConstants.NULL_BYTE);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isNull();
    }

    @Test
    public void testBoxCharNullReturnsNull() {
        final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(1);
        chunk.set(0, QueryConstants.NULL_CHAR);
        assertThat(DictionaryChunkWriter.boxValue(chunk, 0)).isNull();
    }
}
