package org.slf4j.impl;

import io.deephaven.internal.DeephavenLoggerFactory;
import org.slf4j.ILoggerFactory;
import org.slf4j.spi.LoggerFactoryBinder;

// DELETE these static classes, update to service loader, if we update to alpha slf4j
public final class StaticLoggerBinder implements LoggerFactoryBinder {

    // to avoid constant folding by the compiler, this field must *not* be final
    public static String REQUESTED_API_VERSION = "1.7.30"; // !final

    private static final StaticLoggerBinder SINGLETON = new StaticLoggerBinder();

    /**
     * Return the singleton of this class.
     *
     * @return the StaticLoggerBinder singleton
     */
    public static StaticLoggerBinder getSingleton() {
        return SINGLETON;
    }

    @Override
    public final ILoggerFactory getLoggerFactory() {
        return DeephavenLoggerFactory.INSTANCE;
    }

    @Override
    public final String getLoggerFactoryClassStr() {
        return DeephavenLoggerFactory.class.getName();
    }
}
