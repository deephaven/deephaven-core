//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.clock;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

/**
 * A marker interface for {@link Clock} that designates implementations as system clocks.
 */
public interface SystemClock extends Clock {

    String KEY = "deephaven.systemClock";
    String DEFAULT = "default";
    String SERVICE_LOADER = "serviceLoader";
    String SYSTEM_UTC = "systemUTC";
    String SYSTEM_MILLIS = "systemMillis";

    /**
     * Creates a new system clock, or returns the singleton instance, based on system property {@value KEY} (uses
     * {@value DEFAULT} if not present).
     * <table border="1">
     * <tr>
     * <th>Property Value</th>
     * <th>Logic</th>
     * </tr>
     * <tr>
     * <td>{@value DEFAULT}</td>
     * <td>{@code serviceLoader().orElse(systemUTC())}</td>
     * </tr>
     * <tr>
     * <td>{@value SERVICE_LOADER}</td>
     * <td>{@code serviceLoader().orElseThrow()}</td>
     * </tr>
     * <tr>
     * <td>{@value SYSTEM_UTC}</td>
     * <td>{@code systemUTC()}</td>
     * </tr>
     * <tr>
     * <td>{@value SYSTEM_MILLIS}</td>
     * <td>{@code systemMillis()}</td>
     * </tr>
     * <tr>
     * <td>value</td>
     * <td>{@code (SystemClock) Class.forName(value).getDeclaredConstructor().newInstance()}</td>
     * </tr>
     * </table>
     *
     * @return a new instance of the system clock
     * @see #systemUTC()
     * @see #systemMillis()
     * @see #serviceLoader()
     */
    static SystemClock of() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        final String value = System.getProperty(KEY, DEFAULT);
        switch (value) {
            case DEFAULT:
                return serviceLoader().orElse(systemUTC());
            case SERVICE_LOADER:
                return serviceLoader().orElseThrow(() -> new IllegalStateException("Unable to load clock"));
            case SYSTEM_UTC:
                return systemUTC();
            case SYSTEM_MILLIS:
                return systemMillis();
            default:
                return (SystemClock) Class.forName(value).getDeclaredConstructor().newInstance();
        }
    }

    /**
     * The system clock, based off of {@link java.time.Clock#systemUTC()}.
     *
     * @return the system clock
     * @see SystemClockUtc
     */
    static SystemClock systemUTC() {
        return SystemClockUtc.INSTANCE;
    }

    /**
     * The system clock, based off of {@link System#currentTimeMillis()}.
     *
     * @return the millis-based system clock
     * @see SystemClockMillis
     */
    static SystemClock systemMillis() {
        return SystemClockMillis.INSTANCE;
    }

    /**
     * Uses {@link ServiceLoader#load(Class)} with {@link SystemClock SystemClock.class} to load a system clock.
     *
     * <p>
     * If a {@link ServiceConfigurationError} is thrown, the error will be logged to {@link System#err} and
     * {@link Optional#empty()} will be returned.
     *
     * @return the service loader system clock
     * @throws IllegalStateException if more than one system clock is registered
     */
    static Optional<SystemClock> serviceLoader() {
        final Iterator<SystemClock> it = ServiceLoader.load(SystemClock.class).iterator();
        if (!it.hasNext()) {
            return Optional.empty();
        }
        final SystemClock impl;
        try {
            impl = it.next();
        } catch (ServiceConfigurationError e) {
            e.printStackTrace(System.err);
            return Optional.empty();
        }
        if (it.hasNext()) {
            throw new IllegalStateException(
                    "More than one SystemClock has been registered as ServiceLoader implementation");
        }
        return Optional.of(impl);
    }
}
