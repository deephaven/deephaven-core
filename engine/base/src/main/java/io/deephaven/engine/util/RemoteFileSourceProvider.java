//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for providing remote resources to the ClassLoader.
 * Plugins can implement this interface and register with RemoteFileSourceClassLoader
 * to provide resources from remote sources.
 */
public interface RemoteFileSourceProvider {
    /**
     * Check if this provider can source the given resource.
     *
     * @param resourceName the name of the resource to check (e.g., "com/example/MyClass.groovy")
     * @return a CompletableFuture that resolves to true if this provider can handle the resource, false otherwise
     */
    CompletableFuture<Boolean> canSourceResource(String resourceName);

    /**
     * Request a resource from the remote source.
     *
     * @param resourceName the name of the resource to fetch (e.g., "com/example/MyClass.groovy")
     * @return a CompletableFuture containing the resource bytes, or null if not found
     */
    CompletableFuture<byte[]> requestResource(String resourceName);

    /**
     * Check if this provider is currently active and should be used for resource requests.
     *
     * @return true if this provider is active, false otherwise
     */
    boolean isActive();
}

