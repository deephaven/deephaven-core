/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.proto.DeephavenChannel;

import java.util.concurrent.CompletableFuture;

/**
 * A session represents a client-side connection to a Deephaven server.
 */
public interface Session
        extends AutoCloseable, ApplicationService, ConsoleService, InputTableService, ObjectService, TableService,
        ConfigService {

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
     * Advanced usage, creates a new table export ID for {@code this} session, but must be managed by the caller. Useful
     * for more advanced integrations, particularly around doPut. Callers are responsible for {@link #release(ExportId)
     * releasing} the export ID if necessary.
     *
     * @return the new export ID
     * @see #release(ExportId)
     */
    ExportId newExportId();

    /**
     * Releases an export ID.
     *
     * @param exportId the export ID
     * @return the future
     */
    CompletableFuture<Void> release(ExportId exportId);

    // ----------------------------------------------------------

    /**
     * Makes a copy from a source ticket and publishes to a result ticket. Neither the source ticket, nor the
     * destination ticket, need to be a client managed ticket.
     *
     * @param resultId the result id
     * @param sourceId the source id
     * @return the future
     */
    CompletableFuture<Void> publish(HasTicketId resultId, HasTicketId sourceId);

    // ----------------------------------------------------------

    /**
     * Exports {@code typedTicket} to a client-managed server object.
     *
     * @param typedTicket the typed ticket
     * @return the future
     */
    CompletableFuture<? extends ServerObject> export(HasTypedTicket typedTicket);

    // ----------------------------------------------------------

    /**
     * The authenticated channel.
     *
     * @return the authenticated channel
     */
    DeephavenChannel channel();
}
