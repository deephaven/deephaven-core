//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeConsumer;
import io.deephaven.util.datastructures.LongSizedDataStructure;

public interface RowSequence extends SafeCloseable, LongSizedDataStructure {
    long NULL_ROW_KEY = -1L;
    boolean isEmpty();
    long lastRowKey();
    boolean forEachRowKey(LongAbortableConsumer lac);
    default void forAllRowKeys(java.util.function.LongConsumer lc) {
        forEachRowKey((final long v) -> {
            lc.accept(v);
            return true;
        });
    }

    void forAllRowKeyRanges(LongRangeConsumer lrc);
}
