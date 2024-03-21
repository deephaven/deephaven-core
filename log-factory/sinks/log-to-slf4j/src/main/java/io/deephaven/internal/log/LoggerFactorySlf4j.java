//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.internal.log;

import com.google.auto.service.AutoService;
import io.deephaven.io.logger.Logger;

@AutoService(LoggerFactory.class)
public final class LoggerFactorySlf4j implements LoggerFactory {

    @Override
    public final Logger create(String name) {
        return new LoggerSlf4j(org.slf4j.LoggerFactory.getLogger(name));
    }
}
