//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import java.net.URI;
import java.nio.file.Path;

import org.eclipse.jetty.util.resource.Resource;

/**
 * An empty {@link Resource} that always indicates non-existence. This is mostly needed to get around a bug in
 * {@link org.eclipse.jetty.util.resource.CombinedResource} where it does not handle null resources correctly.
 */
public class EmptyResource extends Resource {
    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public Path getPath() {
        return null;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isReadable() {
        return false;
    }

    @Override
    public URI getURI() {
        return null;
    }

    @Override
    public String getName() {
        return "";
    }

    @Override
    public String getFileName() {
        return "";
    }

    @Override
    public Resource resolve(String subUriPath) {
        return null;
    }
}
