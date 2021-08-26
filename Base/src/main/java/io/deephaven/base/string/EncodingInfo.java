/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.nio.charset.*;

/**
 * Encoding info enum, to help code determine what assumptions it can make about a CharSet, as well as simplification
 * for encode/decode operations.
 */
public enum EncodingInfo implements LogOutputAppendable {

    US_ASCII(StandardCharsets.US_ASCII, true), ISO_8859_1(StandardCharsets.ISO_8859_1, true), UTF_8(
            StandardCharsets.UTF_8, false), UTF_16BE(StandardCharsets.UTF_16BE,
                    false), UTF_16LE(StandardCharsets.UTF_16LE, false), UTF_16(StandardCharsets.UTF_16, false);

    private final Charset charset;
    private final String encodingName;
    private final boolean isSimple; // Can we simply cast single bytes to single chars without breaking anything? Not
                                    // multi-byte, subset of UCS-2.

    private final ThreadLocal<SoftReference<CharsetEncoder>> encoder;
    private final ThreadLocal<SoftReference<CharsetDecoder>> decoder;

    EncodingInfo(@NotNull final Charset charset,
            @NotNull final String encodingName,
            final boolean isSimple) {
        this.charset = Require.neqNull(charset, "charSet");
        this.encodingName = Require.neqNull(encodingName, "encodingName");
        this.isSimple = isSimple;
        encoder = ThreadLocal.withInitial(() -> new SoftReference<>(makeEncoder()));
        decoder = ThreadLocal.withInitial(() -> new SoftReference<>(makeDecoder()));

    }

    EncodingInfo(@NotNull final Charset charset,
            final boolean isSimple) {
        this(charset, charset.name(), isSimple);
    }

    @SuppressWarnings("unused")
    EncodingInfo(@NotNull final String encodingName,
            final boolean isSimple) {
        this(Charset.forName(encodingName), encodingName, isSimple);
    }

    public String getEncodingName() {
        return encodingName;
    }

    /**
     * Can this encoding info's charset be encoded or decoded by simple linear assignment of char->byte or byte->char.
     * 
     * @return Whether this encoding info's charset is simple
     */
    public boolean isSimple() {
        return isSimple;
    }

    public Charset getCharset() {
        return charset;
    }

    private CharsetEncoder makeEncoder() {
        return charset.newEncoder().onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
    }

    /**
     * <p>
     * Get a thread local encoder for this encoding info.
     *
     * <p>
     * The encoder will be setup to replace malformed input or unmappable characters, and these settings should be
     * restored if changed.
     *
     * @return A thread local encoder for this encoding info
     */
    public CharsetEncoder getEncoder() {
        CharsetEncoder encoder = this.encoder.get().get();
        if (encoder == null) {
            this.encoder.set(new SoftReference<>(encoder = makeEncoder()));
        }
        return encoder;
    }

    private CharsetDecoder makeDecoder() {
        return charset.newDecoder().onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
    }

    /**
     * <p>
     * Get a thread local decoder for this encoding info.
     *
     * <p>
     * The decoder will be setup to replace malformed input or unmappable characters, and these settings should be
     * restored if changed.
     *
     * @return A thread local decoder for this encoding info
     */
    public CharsetDecoder getDecoder() {
        CharsetDecoder decoder = this.decoder.get().get();
        if (decoder == null) {
            this.decoder.set(new SoftReference<>(decoder = makeDecoder()));
        }
        return decoder;
    }

    public byte[] encode(@NotNull final String value) {
        return value.getBytes(charset);
    }

    public String decode(@NotNull final byte[] value) {
        return new String(value, charset);
    }

    public String decode(@NotNull final byte[] value, final int offset, final int length) {
        return new String(value, offset, length, charset);
    }

    @Override
    public LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(encodingName);
    }
}
