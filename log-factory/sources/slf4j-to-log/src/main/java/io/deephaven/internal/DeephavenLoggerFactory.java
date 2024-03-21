//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.internal;

import io.deephaven.internal.log.LoggerFactory;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

public enum DeephavenLoggerFactory implements ILoggerFactory {
    INSTANCE;

    @Override
    public final Logger getLogger(String name) {
        return new DeephavenLogger(LoggerFactory.getLogger(name), name);
    }
}
