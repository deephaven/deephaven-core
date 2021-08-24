package io.deephaven.configuration;

import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Uses {@link ServiceLoader#load(Class)} to find the best {@link PropertyInputStreamLoader} (the
 * loader with the smallest priority).
 */
public class PropertyInputStreamLoaderFactory {

    public static PropertyInputStreamLoader newInstance() {
        final List<PropertyInputStreamLoader> loaders = StreamSupport
            .stream(ServiceLoader.load(PropertyInputStreamLoader.class).spliterator(), false)
            .collect(Collectors.toList());
        if (loaders.isEmpty()) {
            final String message = String.format(
                "Unable to find any provided implementations for %s. This should not happen - we expect at least %s to be on the classpath.",
                PropertyInputStreamLoader.class.getName(),
                PropertyInputStreamLoaderTraditional.class.getName());
            throw new IllegalStateException(message);
        }
        final long distinctCount = loaders.stream()
            .mapToLong(PropertyInputStreamLoader::getPriority)
            .distinct()
            .count();
        if (distinctCount != loaders.size()) {
            final String propertyInputStreamLoadersDebugInfo = loaders.stream()
                .map(c -> c.getClass().getName() + ":" + c.getPriority())
                .collect(Collectors.joining(",", "[", "]"));
            final String message = String.format(
                "Unable to return the appropriate %s - at least two of the implementations have equal priorities, and that is not allowed. %s",
                PropertyInputStreamLoader.class.getName(),
                propertyInputStreamLoadersDebugInfo);
            throw new IllegalStateException(message);
        }
        return loaders.stream()
            .min(Comparator.comparing(PropertyInputStreamLoader::getPriority))
            .get(); // we know this will be present since loaders is not empty
    }
}
