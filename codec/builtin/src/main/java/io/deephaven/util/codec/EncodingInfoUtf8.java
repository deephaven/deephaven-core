//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Internal, stripped down version of io.deephaven.base.string.EncodingInfo, so
 * {@link CodecUtil#getUtf8String(ByteBuffer)} and {@link CodecUtil#putUtf8String(ByteBuffer, String)} can still use it.
 */
final class EncodingInfoUtf8 {

    private static final ThreadLocal<SoftReference<CharsetEncoder>> ENCODER =
            ThreadLocal.withInitial(() -> new SoftReference<>(makeEncoder()));
    private static final ThreadLocal<SoftReference<CharsetDecoder>> DECODER =
            ThreadLocal.withInitial(() -> new SoftReference<>(makeDecoder()));

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static CharsetEncoder makeEncoder() {
        return CHARSET.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
    }

    public static CharsetEncoder getEncoder() {
        CharsetEncoder encoder = EncodingInfoUtf8.ENCODER.get().get();
        if (encoder == null) {
            EncodingInfoUtf8.ENCODER.set(new SoftReference<>(encoder = makeEncoder()));
        }
        return encoder;
    }

    private static CharsetDecoder makeDecoder() {
        return CHARSET.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
    }

    public static CharsetDecoder getDecoder() {
        CharsetDecoder decoder = EncodingInfoUtf8.DECODER.get().get();
        if (decoder == null) {
            EncodingInfoUtf8.DECODER.set(new SoftReference<>(decoder = makeDecoder()));
        }
        return decoder;
    }
}
