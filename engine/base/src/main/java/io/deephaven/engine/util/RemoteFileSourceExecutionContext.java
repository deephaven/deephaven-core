//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import java.util.List;
import java.util.Objects;

/**
 * An immutable declaration by one client that the named resources should be sourced from it rather than from the local
 * classpath, scoped to a single script evaluation. See {@link RemoteFileSourceClassLoader} for the lifecycle.
 *
 * <p>
 * The paths and the dirty flag are captured here at declaration time rather than read back from the provider, so that a
 * client declaring again cannot change what an evaluation already underway is resolving.
 */
public final class RemoteFileSourceExecutionContext {
    private final RemoteFileSourceProvider provider;
    private final List<String> resourcePaths;
    private final boolean dirty;

    /**
     * @param provider the provider that will service resource requests for this evaluation
     * @param resourcePaths resource paths (e.g. "package/MyScript.groovy") to resolve from the provider
     * @param dirty whether the remote sources have changed since the previous declaration
     */
    public RemoteFileSourceExecutionContext(final RemoteFileSourceProvider provider,
            final List<String> resourcePaths, final boolean dirty) {
        this.provider = Objects.requireNonNull(provider, "provider");
        this.resourcePaths = List.copyOf(resourcePaths);
        this.dirty = dirty;
    }

    /**
     * @return the provider that will service resource requests for this evaluation
     */
    public RemoteFileSourceProvider getProvider() {
        return provider;
    }

    /**
     * @return whether any resource paths were declared
     */
    public boolean hasConfiguredResources() {
        return !resourcePaths.isEmpty();
    }

    /**
     * @return whether the remote sources changed since the previous declaration, so caches must be cleared
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Determines whether the declared resources include the named one. Only Groovy source files are sourced remotely;
     * compiled classes are always resolved locally.
     *
     * @param resourceName the resource being resolved
     * @return true if this resource should be fetched from the provider
     */
    public boolean canSourceResource(final String resourceName) {
        return resourceName.endsWith(".groovy") && resourcePaths.contains(resourceName);
    }

    @Override
    public String toString() {
        return "RemoteFileSourceExecutionContext{resourcePaths=" + resourcePaths + ", dirty=" + dirty + '}';
    }
}
