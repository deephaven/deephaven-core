/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for immutable byte[]-backed String replacements.
 */
public abstract class AbstractCompressedString<TYPE extends AbstractCompressedString> implements StringAlike<TYPE> {
    private static final long serialVersionUID = -2596527344240947333L;

    private static final Charset ENCODING = StandardCharsets.ISO_8859_1;

    private final byte[] data;

    private int cachedHashCode;

    AbstractCompressedString() {
        this.data = new byte[0];
    }

    AbstractCompressedString(final String data) {
        this.data = data.getBytes(ENCODING);
    }

    AbstractCompressedString(final char[] data, final int offset, final int length) {
        this.data = new byte[length];
        for (int bi = 0; bi < length; ++bi) {
            this.data[bi] = (byte) data[offset + bi];
        }
    }

    AbstractCompressedString(final char[] data) {
        this(data, 0, data.length);
    }

    AbstractCompressedString(final ByteBuffer data, final int offset, final int length) {
        final ByteBuffer readOnlyBufferView = data.asReadOnlyBuffer();
        readOnlyBufferView.position(offset);
        readOnlyBufferView.limit(offset + length);
        readOnlyBufferView.get(this.data = new byte[length]);
    }

    AbstractCompressedString(final ByteBuffer data) {
        this(data, data.position(), data.remaining());
    }

    AbstractCompressedString(final byte[] data, final int offset, final int length) {
        this.data = Arrays.copyOfRange(data, offset, offset + length);
    }

    AbstractCompressedString(final byte[] data) {
        this(data, 0, data.length);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Helpers for data conversion
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Convert a String to this type.
     *
     * @param string The String to convert
     * @return A new TYPE with the same contents as String, assuming ISO-8859-1 encoding
     */
    protected abstract TYPE convertValue(String string);

    /**
     * Convert a byte array to this type, assuming ISO-8859-1
     * 
     * @param data The data to convert
     * @param offset The starting index from data to convert
     * @param length The length to convert
     * @return A new TYPE with the same contents as the specified region of data, assuming ISO-8859-1 encoding
     */
    protected abstract TYPE convertValue(byte[] data, int offset, int length);

    private TYPE convertValue(final byte[] data) {
        return convertValue(data, 0, data.length);
    }

    private TYPE[] convertArray(final String... strings) {
        final StringAlike[] result = new StringAlike[strings.length];
        for (int si = 0; si < strings.length; ++si) {
            result[si] = convertValue(strings[si]);
        }
        // noinspection unchecked
        return (TYPE[]) result;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Note: This is an API for trusted code to use. The data array must not be modified.
     * 
     * @return The internal data array for this instance.
     */
    public final byte[] getData() {
        return data;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // CharSequence implementation
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public final int length() {
        return data.length;
    }

    @Override
    public final char charAt(final int index) {
        return (char) (data[index] & 0xFF);
    }

    @Override
    public final CharSequence subSequence(final int start, final int end) {
        return substring(start, end);
    }

    @Override
    @NotNull
    public final String toString() {
        return new String(data, ENCODING);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Comparable implementation and related methods
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public final int compareTo(@NotNull final CharSequence that) {
        return CharSequenceUtils.CASE_SENSITIVE_COMPARATOR.compare(this, that);
    }

    @Override
    public final int hashCode() {
        int hashCode = cachedHashCode;
        if (hashCode == 0 && data.length > 0) {
            // noinspection ForLoopReplaceableByForEach
            for (int bi = 0; bi < data.length; ++bi) {
                hashCode = 31 * hashCode + data[bi];
            }
            cachedHashCode = hashCode;
        }
        return hashCode;
    }

    @Override
    public final boolean equals(final Object that) {
        if (this == that) {
            return true;
        }

        if (that == null) {
            return false;
        }

        if (getClass() != that.getClass()) {
            return false;
        }

        // noinspection unchecked
        final TYPE thatType = (TYPE) that;
        if (data.length != thatType.length()) {
            return false;
        }
        for (int bi = 0; bi < data.length; ++bi) {
            if (data[bi] != thatType.getData()[bi]) {
                return false;
            }
        }
        return true;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // String-alike methods with real implementations (implemented in terms of CharSequences whenever appropriate)
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public final boolean isEmpty() {
        return data.length == 0;
    }

    public final int codePointAt(final int index) {
        return charAt(index);
    }

    public final int codePointBefore(final int index) {
        return charAt(index - 1);
    }

    public final int codePointCount(final int beginIndex, final int endIndex) {
        return endIndex - beginIndex;
    }

    @Override
    public final void getChars(final int srcBegin, final int srcEnd, final char dst[], final int dstBegin) {
        for (int si = srcBegin, di = dstBegin; si < srcEnd; ++si) {
            dst[di++] = charAt(si);
        }
    }

    @Override
    public final byte[] getBytes() {
        return data.clone();
    }

    @Override
    public final boolean contentEquals(@NotNull final CharSequence cs) {
        return data.length == cs.length() && (cs.equals(this) || CharSequenceUtils.contentEquals(this, cs));
    }

    @Override
    public final boolean equalsIgnoreCase(@NotNull final CharSequence that) {
        return CharSequenceUtils.contentEqualsIgnoreCase(this, that);
    }

    @Override
    public final int compareToIgnoreCase(@NotNull final CharSequence that) {
        return CharSequenceUtils.CASE_INSENSITIVE_COMPARATOR.compare(this, that);
    }

    @Override
    public final boolean regionMatches(final boolean ignoreCase,
            final int offset,
            final CharSequence that,
            final int thatOffset,
            final int length) {
        return CharSequenceUtils.regionMatches(ignoreCase, this, offset, that, thatOffset, length);
    }

    @Override
    public final boolean startsWith(@NotNull final CharSequence prefix, final int offset) {
        return CharSequenceUtils.regionMatches(false, this, offset, prefix, 0, prefix.length());
    }

    @Override
    public final boolean startsWith(@NotNull final CharSequence prefix) {
        return startsWith(prefix, 0);
    }

    @Override
    public final boolean endsWith(@NotNull final CharSequence suffix) {
        return startsWith(suffix, data.length - suffix.length());
    }

    @Override
    public final int indexOf(final int ch, int fromIndex) {
        if (fromIndex >= data.length) {
            return -1;
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        for (int ci = fromIndex; ci < data.length; ++ci) {
            if (charAt(ci) == ch) {
                return ci;
            }
        }
        return -1;
    }

    @Override
    public final int indexOf(final int ch) {
        return indexOf(ch, 0);
    }

    @Override
    public final int lastIndexOf(final int ch, int fromIndex) {
        if (fromIndex < 0) {
            return -1;
        }
        if (fromIndex >= data.length) {
            fromIndex = data.length;
        }
        for (int ci = fromIndex; ci >= 0; --ci) {
            if (charAt(ci) == ch) {
                return ci;
            }
        }
        return -1;
    }

    @Override
    public final int lastIndexOf(final int ch) {
        return lastIndexOf(ch, data.length - 1);
    }

    @Override
    public final TYPE substring(final int beginIndex, final int endIndex) {
        if (beginIndex < 0) {
            throw new StringIndexOutOfBoundsException(beginIndex);
        }
        if (endIndex > data.length) {
            throw new StringIndexOutOfBoundsException(endIndex);
        }
        if (beginIndex > endIndex) {
            throw new StringIndexOutOfBoundsException(endIndex - beginIndex);
        }
        // noinspection unchecked
        return (TYPE) (beginIndex == 0 && endIndex == data.length ? this
                : convertValue(data, beginIndex, endIndex - beginIndex));
    }

    @Override
    public final TYPE substring(final int beginIndex) {
        return substring(beginIndex, data.length);
    }

    @Override
    public final TYPE concat(final String other) {
        if (other.length() == 0) {
            // noinspection unchecked
            return (TYPE) this;
        }
        final byte[] otherData = other.getBytes(ENCODING);
        final byte[] concatenatedData = Arrays.copyOf(data, data.length + otherData.length);
        System.arraycopy(otherData, 0, concatenatedData, data.length, otherData.length);
        return convertValue(concatenatedData);
    }

    @Override
    public final TYPE concat(final TYPE other) {
        if (other.length() == 0) {
            // noinspection unchecked
            return (TYPE) this;
        }
        final byte[] concatenatedData = Arrays.copyOf(data, data.length + other.length());
        System.arraycopy(other.getData(), 0, concatenatedData, data.length, other.length());
        return convertValue(concatenatedData);
    }

    @Override
    public final boolean matches(final CharSequence regex) {
        return Pattern.matches(regex.toString(), this);
    }

    @Override
    public final TYPE trim() {
        int bii = 0;
        int eii = data.length - 1;
        while (bii <= eii && charAt(bii) <= ' ') {
            ++bii;
        }
        while (eii > bii && charAt(eii) <= ' ') {
            --eii;
        }
        // noinspection unchecked
        return (TYPE) (bii == 0 && eii == data.length - 1 ? this : substring(bii, eii + 1));
    }

    @Override
    public final char[] toCharArray() {
        final char[] result = new char[data.length];
        getChars(0, data.length, result, 0);
        return result;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // String-alike method implementations that delegate to String (because they're complicated by encoding or other
    // issues and/or likely not used often)
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public final int indexOf(final CharSequence cs, final int fromIndex) {
        return toString().indexOf(cs.toString(), fromIndex);
    }

    @Override
    public final int indexOf(final CharSequence cs) {
        return indexOf(cs, 0);
    }

    @Override
    public final int lastIndexOf(final CharSequence cs, final int fromIndex) {
        return toString().lastIndexOf(cs.toString(), fromIndex);
    }

    @Override
    public final int lastIndexOf(final CharSequence cs) {
        return lastIndexOf(cs, cs.length());
    }

    @Override
    public final TYPE replace(final char oldChar, final char newChar) {
        return convertValue(toString().replace(oldChar, newChar));
    }

    @Override
    public final boolean contains(final CharSequence cs) {
        return indexOf(cs) > -1;
    }

    @Override
    public final TYPE replaceFirst(final CharSequence regex, final CharSequence replacement) {
        return convertValue(Pattern.compile(regex.toString()).matcher(this).replaceFirst(replacement.toString()));
    }

    @Override
    public final TYPE replaceAll(final CharSequence regex, final CharSequence replacement) {
        return convertValue(Pattern.compile(regex.toString()).matcher(this).replaceAll(replacement.toString()));
    }

    @Override
    public final TYPE replace(final CharSequence target, final CharSequence replacement) {
        return convertValue(Pattern.compile(target.toString(), Pattern.LITERAL).matcher(toString())
                .replaceAll(Matcher.quoteReplacement(replacement.toString())));
    }

    @Override
    public final TYPE[] split(final CharSequence regex, final int limit) {
        return convertArray(toString().split(regex.toString(), limit));
    }

    @Override
    public final TYPE[] split(final CharSequence regex) {
        return split(regex, 0);
    }

    @Override
    public final TYPE toLowerCase(final Locale locale) {
        return convertValue(toString().toLowerCase(locale));
    }

    @Override
    public final TYPE toLowerCase() {
        return toLowerCase(Locale.getDefault());
    }

    @Override
    public final TYPE toUpperCase(final Locale locale) {
        return convertValue(toString().toUpperCase(locale));
    }

    @Override
    public final TYPE toUpperCase() {
        return toUpperCase(Locale.getDefault());
    }

    // -----------------------------------------------------------------------------------------------------------------
    // String-alike method implementations that are explicitly unsupported.
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public final TYPE intern() {
        throw new UnsupportedOperationException();
    }
}
