package io.deephaven.configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * A {@link PropertyInputStreamLoader} implementation that first searches for the property file as a
 * classpath resource, and then via the filesystem. The priority is 100.
 */
public class PropertyInputStreamLoaderTraditional implements PropertyInputStreamLoader {

    public PropertyInputStreamLoaderTraditional() {}

    @Override
    public long getPriority() {
        return 100;
    }

    @Override
    public InputStream openConfiguration(String name) {
        final InputStream viaResources = getClass().getResourceAsStream("/" + name);
        if (viaResources != null) {
            return viaResources;
        }
        try {
            return new FileInputStream(name);
        } catch (FileNotFoundException e) {
            throw new ConfigurationException("Could not load property file: " + name, e);
        }
    }
}
