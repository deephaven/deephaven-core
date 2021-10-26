package io.deephaven.grpc_api.uri;

import io.deephaven.db.tables.Table;

import java.net.URI;
import java.util.Set;

/**
 * A table resolver resolves {@link URI URIs} into {@link Table tables}.
 */
public interface TableResolver {

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
     * Resolve {@code uri} into a table.
     *
     * @param uri the URI
     * @return the table
     * @throws InterruptedException if the current thread is interrupted
     */
    Table resolve(URI uri) throws InterruptedException;
}
