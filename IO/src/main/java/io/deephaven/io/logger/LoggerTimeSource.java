/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.io.logger;

public interface LoggerTimeSource {
    long currentTimeMicros();
}
