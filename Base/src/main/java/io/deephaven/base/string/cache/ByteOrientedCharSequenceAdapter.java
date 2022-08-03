/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.string.cache;

/**
 * Common parent for CharSequenceAdapters that store chars as single bytes (for simple encodings).
 */
public abstract class ByteOrientedCharSequenceAdapter extends CharSequenceAdapter {

    /**
     * Re-usable character array for String construction.
     */
    private char stringConstructionBuffer[] = new char[0];

    /**
     * @return A new String consistent with this CharSequenceAdapter.
     */
    @Override
    protected final String makeString() {
        final int length = length();
        if (stringConstructionBuffer.length < length) {
            stringConstructionBuffer = new char[length];
        }
        for (int ci = 0; ci < length; ++ci) {
            stringConstructionBuffer[ci] = charAt(ci);
        }
        return new String(stringConstructionBuffer, 0, length);
    }
}
