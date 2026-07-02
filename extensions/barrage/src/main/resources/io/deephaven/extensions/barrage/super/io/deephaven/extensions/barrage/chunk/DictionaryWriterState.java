//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import java.util.List;

/**
 * GWT super-source stub for {@code DictionaryWriterState}.
 * <p>
 * Dictionary-encoded (DE) writing is <b>not supported</b> in the GWT/browser environment. This stub exists solely to
 * allow {@link io.deephaven.extensions.barrage.BarrageMessageWriterImpl} to compile under GWT. No instances of any
 * implementing class are ever created in GWT, so none of these methods are ever called.
 */
public interface DictionaryWriterState {

    long getDictId();

    int indexForObject(Object value);
    int indexForByte(byte v);
    int indexForChar(char v);
    int indexForShort(short v);
    int indexForInt(int v);
    int indexForLong(long v);
    int indexForFloat(float v);
    int indexForDouble(double v);

    boolean hasDelta();

    boolean needsFullBatch();

    List<Object> getDeltaValues();

    void resetDelta();

    int totalSize();

    void reset();
}
