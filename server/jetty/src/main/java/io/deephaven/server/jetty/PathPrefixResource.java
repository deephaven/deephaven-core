//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.util.resource.Resource;

/**
 * A `Resource` wrapper that only resolves URIs that start with a specific path prefix.
 * This is useful for creating resources that are only valid for a specific sub-path of the server without requiring
 * servlet apis.
 */
public class PathPrefixResource extends WrappedResource {
    /**
     * Wraps a Resource with a `PathPrefixResource` that resolves only for URIs starting with the given pathPrefix.
     *
     * @param pathPrefix the path prefix to match against
     * @param wrapped the Resource to wrap
     * @return a `PathPrefixResource` wrapping the given Resource, or null if the wrapped Resource is null
     */
    public static PathPrefixResource wrap(String pathPrefix, Resource wrapped) {
        if (wrapped == null) {
            return null;
        }

        // already wrapped, return as-is
        if (wrapped instanceof PathPrefixResource rpr) {
            return rpr;
        }
        
        return new PathPrefixResource(pathPrefix, wrapped);
    }

    private final String pathPrefix;

    private PathPrefixResource(String pathPrefix, Resource wrapped) {
        super(wrapped);
        this.pathPrefix = pathPrefix;
    }

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
