package io.deephaven.uri;

import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.uri.TableResolversInstance;

import java.net.URI;

/**
 * The top-level entrypoint for resolving {@link URI URIs} into {@link Table tables}. Uses the global table resolvers
 * instance from {@link TableResolversInstance#get()}.
 *
 * <p>
 * The exact parsing logic will depend on which {@link io.deephaven.uri.TableResolver table resolvers} are installed.
 *
 * @see StructuredUri structured URI
 */
public class ResolveTools {

    /**
     * Resolves the {@code uri} into a table.
     *
     * @param uri the URI
     * @return the table
     */
    public static Table resolve(String uri) throws InterruptedException {
        return resolve(URI.create(uri));
    }

    /**
     * Resolves the {@code uri} into a table.
     *
     * @param uri the URI
     * @return the table
     */
    public static Table resolve(URI uri) throws InterruptedException {
        return TableResolversInstance.get().resolve(uri);
    }
}
