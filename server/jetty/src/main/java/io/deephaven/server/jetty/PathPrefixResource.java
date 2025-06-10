//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.util.resource.Resource;

import java.util.Objects;

/**
 * A `Resource` wrapper that only resolves URIs that start with a specific path prefix. This is useful for creating
 * resources that are only valid for a specific sub-path of the server without requiring servlet apis.
 */
public class PathPrefixResource extends WrappedResource {
    public PathPrefixResource(String pathPrefix, Resource wrapped) {
        super(wrapped);
        Objects.requireNonNull(wrapped, "wrapped Resource cannot be null");
        this.pathPrefix = pathPrefix;
    }

    private final String pathPrefix;

    @Override
    public Resource resolve(String subUriPath) {
        if (!subUriPath.startsWith(pathPrefix)) {
            // It would be nice to just return null here, but CombinedResource has a bug where it does not properly
            // handle null resources, so we return an empty resource instead.
            return new EmptyResource();
        }

        return super.resolve(subUriPath.substring(pathPrefix.length()));
    }
}
