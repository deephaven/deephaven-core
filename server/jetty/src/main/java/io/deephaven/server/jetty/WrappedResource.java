//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import java.net.URI;
import java.nio.file.Path;

import org.eclipse.jetty.util.resource.Resource;

/**
 * Simple wrapper around the Jetty {@link Resource} type. Can be extended to target specific method overrides.
 */
public abstract class WrappedResource extends Resource {
    protected final Resource wrapped;

    protected WrappedResource(Resource wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Path getPath() {
        return wrapped.getPath();
    }

    @Override
    public boolean isContainedIn(Resource r) {
        return wrapped.isContainedIn(r);
    }

    @Override
    public boolean exists() {
        return wrapped.exists();
    }

    @Override
    public boolean isDirectory() {
        return wrapped.isDirectory();
    }

    @Override
    public boolean isReadable() {
        return wrapped.isReadable();
    }

    @Override
    public long length() {
        return wrapped.length();
    }

    @Override
    public URI getURI() {
        return wrapped.getURI();
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public String getFileName() {
        return wrapped.getFileName();
    }

    @Override
    public Resource resolve(String subUriPath) {
        return wrapped.resolve(subUriPath);
    }

    @Override
    public String toString() {
        // Jetty's CachedContentFactory.CachedHttpContent requires that toString return the underlying URL found on
        // disk, or else the mime lookup from content type won't resolve anything.
        return wrapped.toString();
    }

    @Override
    public int hashCode() {
        // As with toString, delegating this to the wrapped instance, just in case there is some specific, expected
        // behavior
        return wrapped.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        // As with toString, delegating this to the wrapped instance, just in case there is some specific, expected
        // behavior
        return wrapped.equals(obj);
    }
}
