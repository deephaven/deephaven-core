//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.logger;

public interface LogBufferRecordListener {
    void record(final LogBufferRecord record);
}
