package io.deephaven.internal.log;

import io.deephaven.io.logger.Logger;

public final class LoggerFactorySlf4j implements LoggerFactory {

    @Override
    public final Logger create(String name) {
        return new LoggerSlf4j(org.slf4j.LoggerFactory.getLogger(name));
    }
}
