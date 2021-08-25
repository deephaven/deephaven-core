package io.deephaven.internal.log;

import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;

enum LoggerFactoryServiceLoaderImpl {
    INSTANCE;

    private final LoggerFactory instance;

    LoggerFactoryServiceLoaderImpl() {
        Bootstrap.log(getClass(),
                String.format("searching for '%s'...", LoggerFactory.class.getName()));
        final Iterator<LoggerFactory> it = ServiceLoader.load(LoggerFactory.class).iterator();
        if (!it.hasNext()) {
            throw new IllegalStateException("No LoggerFactory found via ServiceLoader");
        }
        LoggerFactory factory = it.next();
        Bootstrap.log(getClass(), String.format("found '%s'", factory.getClass().getName()));
        if (it.hasNext()) {
            while (it.hasNext()) {
                factory = it.next();
                Bootstrap.log(getClass(),
                        String.format("found '%s'", factory.getClass().getName()));
            }
            throw new IllegalStateException("Multiple LoggerFactories found via ServiceLoader");
        }
        this.instance = Objects.requireNonNull(factory);
    }

    public LoggerFactory getInstance() {
        return instance;
    }
}
