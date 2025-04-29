//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.resources;

import java.net.URL;

/**
 * Represents a set of resources that can be provided by the HTTP server. Priority will be controlled by configuration.
 */
public abstract class ServerResource {

    /**
     * Returns the name of the resource to use in configuration.
     *
     * @return the name of the resource
     */
    public abstract String getName();

    /**
     * A URL that can be provided to Jetty to serve the resources.
     *
     * @return the URL to the resources
     */
    public String getResourceBaseUrl() {
        // Default implementation assumes that the current class is in the same jar as the resources,
        // and that there exists a META-INF/resources directory in the jar containing the resources.
        return getResourceFromClasspath("/META-INF/resources", getClass());
    }

    protected static String getResourceFromClasspath(String resourceName, Class<?> clazz) {
        URL resource = clazz.getResource(clazz.getSimpleName() + ".class");
        return resource.toExternalForm().replaceAll("!.*\\.class$", "!" + resourceName);
    }
}
