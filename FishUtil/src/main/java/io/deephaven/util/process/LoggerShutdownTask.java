/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.process;

import io.deephaven.io.log.LogSink;

public class LoggerShutdownTask extends OnetimeShutdownTask {

    @Override
    protected void shutdown() {
        LogSink.Shutdown.shutdown();
    }
}
