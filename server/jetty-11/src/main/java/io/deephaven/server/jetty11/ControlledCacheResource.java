//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import org.eclipse.jetty.util.resource.Resource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;

/**
 * Simple wrapper around the Jetty Resource type, to grant us control over caching features. The current implementation
 * only removes the last-modified value, but a future version could provide a "real" weak/strong etag.
 */
public class ControlledCacheResource extends Resource {
    public static ControlledCacheResource wrap(Resource wrapped) {
        if (wrapped instanceof ControlledCacheResource) {
            return (ControlledCacheResource) wrapped;
        }
        return new ControlledCacheResource(wrapped);
    }

    private final Resource wrapped;

    private ControlledCacheResource(Resource wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public boolean isContainedIn(Resource r) throws MalformedURLException {
        return wrapped.isContainedIn(r);
    }

    @Override
    public void close() {
        wrapped.close();
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
    public long lastModified() {
        // Always return -1, so that we don't get the build system timestamp. In theory we could return the app startup
        // time as well, so that clients that connect don't need to revalidate quite as often, but this could have other
        // side effects such as in load balancing with a short-lived old build against a seconds-older new build.
        return -1;
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
    public File getFile() throws IOException {
        return wrapped.getFile();
    }

    @Override
    public String getName() {
        return wrapped.getName();
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return wrapped.getInputStream();
    }

    @Override
    public ReadableByteChannel getReadableByteChannel() throws IOException {
        return wrapped.getReadableByteChannel();
    }

    @Override
    public boolean delete() throws SecurityException {
        return wrapped.delete();
    }

    @Override
    public boolean renameTo(Resource dest) throws SecurityException {
        return wrapped.renameTo(dest);
    }

    @Override
    public String[] list() {
        return wrapped.list();
    }

    @Override
    public Resource addPath(String path) throws IOException, MalformedURLException {
        // Re-wrap any instance that might be returned
        return wrap(wrapped.addPath(path));
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
