/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import io.deephaven.base.string.EncodingInfo;
import org.apache.commons.io.ByteOrderMark;
import org.jetbrains.annotations.NotNull;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;

public class EncodingUtil {
    @SuppressWarnings("WeakerAccess")
    public static final ByteOrderMark[] EMPTY_BOM_ARRAY = new ByteOrderMark[0];

    /**
     * Get the {@link EncodingInfo} associated with a particular {@link Charset}
     *
     * @param charSet The charset
     * @return the matching {@link EncodingInfo}
     * @throws IllegalArgumentException if there is no associated encoding
     */
    @NotNull
    public static EncodingInfo getEncodingInfoForCharset(@NotNull Charset charSet) throws IllegalArgumentException {
        return getEncodingInfoForCharset(charSet.name());
    }

    /**
     * Get the {@link EncodingInfo} associated with a particular charset name
     *
     * @param charsetName the charset
     * @return the matching {@link EncodingInfo}
     * @throws IllegalArgumentException if there is no associated encoding
     */
    public static EncodingInfo getEncodingInfoForCharset(@NotNull String charsetName) {
        return Arrays.stream(EncodingInfo.values())
                .filter(info -> info.getCharset().name().equals(charsetName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No EncodingInfo for " + charsetName));
    }

    /**
     * Get an array containing the possible {@link ByteOrderMark byte order marks} that could be present within a file
     * of the specified encoding. This is intended for use with {@link org.apache.commons.io.input.BOMInputStream}
     *
     * @param encoding The encoding.
     * @return An array containing the possible {@link ByteOrderMark BOMs} for the encoding.
     */
    @NotNull
    public static ByteOrderMark[] getBOMsForEncoding(EncodingInfo encoding) {
        switch (encoding) {
            case UTF_8:
                return new ByteOrderMark[] {ByteOrderMark.UTF_8};
            case UTF_16BE:
                return new ByteOrderMark[] {ByteOrderMark.UTF_16BE};
            case UTF_16LE:
                return new ByteOrderMark[] {ByteOrderMark.UTF_16LE};
            case UTF_16:
                return new ByteOrderMark[] {ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_16LE};
        }

        return EMPTY_BOM_ARRAY;
    }

    /**
     * Encode the given string in UTF-8 format into the given ByteBuffer. The string is encoded as an int length
     * followed by the encoded bytes.
     *
     * @param destination a ByteBuffer in which to encode the string. The buffer must be big enough for the encoded
     *        string.
     * @param value the String value to encode.
     * @throws BufferOverflowException if the destination isn't big enough.
     */
    public static void putUtf8String(@NotNull final ByteBuffer destination, @NotNull final String value) {
        final int initialPosition = destination.position();
        destination.position(initialPosition + Integer.BYTES);
        final CharsetEncoder encoder = EncodingInfo.UTF_8.getEncoder().reset();
        if (!encoder.encode(CharBuffer.wrap(value), destination, true).isUnderflow()
                || !encoder.flush(destination).isUnderflow()) {
            throw new BufferOverflowException();
        }
        destination.putInt(initialPosition, destination.position() - initialPosition - Integer.BYTES);
    }

    /**
     * Extract a UTF-8 encoded string from the given buffer. The buffer must be positioned at the start of the encoding,
     * which is an int length followed by the UTF-8 encoded bytes. The buffer is advanced to the end of the string.
     *
     * @param source a ByteBuffer positioned at the string encoded as length + UTF-8 encoded bytes.
     * @return a new String extracted from the buffer
     * @throws BufferUnderflowException if there isn't enough data in the source ByteBuffer
     * @throws IllegalArgumentException if there is a decoding error
     */
    public static String getUtf8String(@NotNull final ByteBuffer source) {
        final int length = source.getInt();
        final int initialLimit = source.limit();
        final CharsetDecoder decoder = EncodingInfo.UTF_8.getDecoder().reset();
        if (length > source.remaining()) {
            throw new BufferUnderflowException();
        }
        source.limit(source.position() + length);
        final String result;
        try {
            result = decoder.decode(source).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Unexpectedly failed to decode input bytes", e);
        }
        source.limit(initialLimit);
        return result;
    }
}
