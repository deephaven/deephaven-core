package io.deephaven.uri.resolver;

import java.net.URI;
import java.util.Set;

/**
 * A URI resolver resolves {@link URI URIs} into {@link Object objects}.
 */
public interface UriResolver {

    /**
     * The supported schemes.
     *
     * @return the schemes
     */
    Set<String> schemes();

    /**
     * Returns true if the resolver can {@link #resolve(URI) resolve} the {@code uri}.
     *
     * @param uri the uri
     * @return true if this resolver can resolve uri
     */
    boolean isResolvable(URI uri);

    /**
     * Resolve {@code uri} into an object.
     *
     * @param uri the URI
     * @return the object
     * @throws InterruptedException if the current thread is interrupted
     */
    Object resolve(URI uri) throws InterruptedException;
}
