package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

/**
 * A session represents a client-side connection to a Deephaven server.
 */
public interface Session
        extends AutoCloseable, ApplicationService, ConsoleService, InputTableService, ObjectService, TableService {

    // ----------------------------------------------------------

    /**
     * Closes the session.
     */
    @Override
    void close();

    /**
     * Closes the session.
     *
     * @return the future
     */
    CompletableFuture<Void> closeFuture();

    // ----------------------------------------------------------

    /**
     * Advanced usage, creates a new export ID for {@code this} session, but must be managed by the caller. Useful for
     * more advanced integrations, particularly around doPut. Callers are responsible for {@link #release(ExportId)
     * releasing} the export ID if necessary.
     *
     * @return the new export ID
     * @see #release(ExportId)
     */
    ExportId newExportId();

    /**
     * Releases an export ID.
     *
     * <p>
     * Note: this should <b>only</b> be called in combination with exports returned from {@link #newExportId()}.
     *
     * @param exportId the export ID
     * @return the future
     */
    CompletableFuture<Void> release(ExportId exportId);

    // ----------------------------------------------------------
}
