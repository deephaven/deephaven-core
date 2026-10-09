//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import java.util.concurrent.CompletableFuture;

/**
 * Fetches remote resources on behalf of the {@link RemoteFileSourceClassLoader}. Plugins implement this and declare the
 * resources they can serve via
 * {@link RemoteFileSourceClassLoader#declareExecutionContext(RemoteFileSourceProvider, java.util.List, boolean)}. The
 * class loader decides which provider serves a given evaluation, from the declaration it claimed for that run.
 */
public interface RemoteFileSourceProvider {
    /**
     * Request a resource from the remote source.
     *
     * @param resourceName the name of the resource to fetch (e.g., "com/example/MyClass.groovy")
     * @return a CompletableFuture that completes with the resource bytes, or completes with null if the resource was
     *         not found. The future itself is never null; a provider that cannot service the request returns a future
     *         that completes exceptionally.
     */
    CompletableFuture<byte[]> requestResource(String resourceName);
}
