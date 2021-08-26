package io.deephaven.configuration;

import java.io.InputStream;
import java.util.ServiceLoader;

/**
 * An abstraction for opening property files. Invoked via {@link ParsedProperties#load(String)}.
 *
 * <p>
 * The default implementation is {@link PropertyInputStreamLoaderTraditional}.
 *
 * <p>
 * To override the default, additional {@link PropertyInputStreamLoader} implementations can be added to the classpath
 * and referenced via the {@link ServiceLoader} mechanism.
 */
public interface PropertyInputStreamLoader {

    /**
     * A helper for determining the precedence of the loader.
     *
     * @return the priority - the lower, the better.
     */
    long getPriority();

    /**
     * Opens the property stream represented by the given {@code name}.
     *
     * @param name the name of the prop file
     * @return the input stream
     * @throws ConfigurationException if the property stream cannot be opened
     */
    InputStream openConfiguration(String name) throws ConfigurationException;
}
