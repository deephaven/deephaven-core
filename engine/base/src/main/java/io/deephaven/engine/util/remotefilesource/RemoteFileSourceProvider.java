//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.remotefilesource;

import java.util.concurrent.CompletableFuture;

/**
 * Fetches remote resources on behalf of the {@link RemoteFileSourceClassLoader}. Plugins implement this and declare the
 * resources they can serve via
 * {@link RemoteFileSourceClassLoader#declareRemoteSources(RemoteFileSourceProvider, java.util.List, boolean)}. The
 * class loader decides which provider serves a given evaluation, from the declaration it claimed for that run.
 */
public interface RemoteFileSourceProvider {
    /**
     * Request a resource from the remote source.
     *
     * @param resourceName the name of the resource to fetch (e.g., "com/example/MyClass.groovy")
     * @return a CompletableFuture that completes with the resource bytes, which are empty for an empty file. It never
     *         completes with null: a resource the remote source does not have completes it exceptionally with a
     *         {@link java.io.FileNotFoundException}, and any other failure to service the request completes it
     *         exceptionally as well. The future itself is never null.
     */
    CompletableFuture<byte[]> requestResource(String resourceName);
}
