package io.deephaven.uri;

import io.deephaven.db.tables.Table;

import java.net.URI;
import java.util.Set;

/**
 * A table resolver resolves {@link URI URIs} into {@link Table tables}.
 */
public interface TableResolver extends UriCreator {

    /**
     * The supported schemes.
     *
     * @return the schemes
     */
    Set<String> schemes();

    /**
     * Resolve {@code uri} into a table.
     *
     * @param uri the URI
     * @return the table
     * @throws InterruptedException if the current thread is interrupted
     */
    Table resolve(URI uri) throws InterruptedException;
}
