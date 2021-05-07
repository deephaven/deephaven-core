package io.deephaven.internal.log;

import io.deephaven.io.logger.Logger;

public interface LoggerFactory {
    static Logger getLogger(String name) {
        return getInstance().create(name);
    }

    static Logger getLogger(Class<?> clazz) {
        return getInstance().create(clazz.getName()); // todo
    }

    static LoggerFactory getInstance() {
        return LoggerFactoryServiceLoaderImpl.INSTANCE.getInstance();
    }

    Logger create(String name);
}
