package io.deephaven.fishconfig;

import io.deephaven.configuration.ConfigurationException;
import io.deephaven.configuration.PropertyInputStreamLoader;
import java.io.InputStream;

/**
 * A {@link PropertyInputStreamLoader} that loads the property input stream from resources only. Has
 * priority 0. Useful for unit testing.
 */
public class PropertyInputStreamLoaderResourcesOnly implements PropertyInputStreamLoader {

    @Override
    public long getPriority() {
        return 0;
    }

    @Override
    public InputStream openConfiguration(String filename) {
        final String resourcePath = "/" + filename;
        final InputStream in = getClass().getResourceAsStream(resourcePath);
        if (in == null) {
            final String message = String.format("Unable to find prop file at resource path '%s'",
                resourcePath);
            throw new ConfigurationException(message);
        }
        return in;
    }
}
