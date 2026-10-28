//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.decoder;

import io.deephaven.base.string.EncodingInfo;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link EncodedStringDecoder}.
 */
public class TestEncodedStringDecoder {

    private final EncodedStringDecoder<String> decoder =
            new EncodedStringDecoder<>(String.class, EncodingInfo.UTF_8);

    @Test
    public void testEmptyEntryIsNull() {
        assertNull(decoder.decode(new byte[] {'a'}, 0, 0));
    }

    @Test
    public void testEmptyStringAtOffset() {
        // The single zero byte that encodes the empty string follows a non-zero byte.
        assertEquals("", decoder.decode(new byte[] {'a', 0}, 1, 1));
    }

    @Test
    public void testOneByteStringAtOffset() {
        // A zero byte precedes the one-byte string.
        assertEquals("b", decoder.decode(new byte[] {0, 'b'}, 1, 1));
    }

    @Test
    public void testMultiByteStringAtOffset() {
        final String expected = "é€";
        final byte[] encoded = expected.getBytes(StandardCharsets.UTF_8);
        final byte[] data = new byte[encoded.length + 2];
        System.arraycopy(encoded, 0, data, 2, encoded.length);
        assertEquals(expected, decoder.decode(data, 2, encoded.length));
    }
}
