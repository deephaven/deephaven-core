/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.internal.log;

import io.deephaven.io.logger.Logger;

public interface LoggerFactory {
    static Logger getLogger(String name) {
        return getInstance().create(name);
    }

    static Logger getLogger(Class<?> clazz) {
        return getInstance().create(clazz.getName());
    }

    static LoggerFactory getInstance() {
        return LoggerFactoryServiceLoaderImpl.INSTANCE.getInstance();
    }

    Logger create(String name);
}
