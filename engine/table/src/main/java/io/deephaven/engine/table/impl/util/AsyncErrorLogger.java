//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.BlinkTableTools;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

public class AsyncErrorLogger {
    private static volatile AsyncErrorImpl INSTANCE;

    private static AsyncErrorImpl getInstance() {
        AsyncErrorImpl local;
        if ((local = INSTANCE) == null) {
            synchronized (AsyncErrorImpl.class) {
                if ((local = INSTANCE) == null) {
                    INSTANCE = local = new AsyncErrorImpl();
                }
            }
        }
        return local;
    }

    public static void init() {
        getInstance();
    }

    public static Table getErrorLog() {
        return BlinkTableTools.blinkToAppendOnly(getInstance().blink());
    }

    public static void log(
            Instant time,
            @Nullable TableListener.Entry entry,
            @Nullable TableListener.Entry sourceEntry,
            Throwable originalException) {
        getInstance().add(time, entry, sourceEntry, originalException);
    }
}

