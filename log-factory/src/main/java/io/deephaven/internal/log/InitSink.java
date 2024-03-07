//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.internal.log;

import io.deephaven.io.log.LogSink;
import io.deephaven.io.logger.LogBuffer;

public interface InitSink {
    void accept(LogSink sink, LogBuffer logBuffer);
}
