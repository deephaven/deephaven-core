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
        extends AutoCloseable, ApplicationService, ConsoleService, InputTableService, ObjectService, TableServices,
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
     * Creates a new stateful {@link TableServices} that keeps references to the exports created from said service for
     * executing queries with maximum cacheability. This allows callers to implicitly take advantage of existing exports
     * when they are executing new queries. In the following example, the second query does not need to re-execute from
     * the beginning; it is able to build off of the export for {@code h1} and simply execute the {@code where}
     * operation.
     *
     * <pre>
     * TableServices ts = session.tableServices();
     * TableHandle h1 = ts.execute(TableSpec.emptyTable(42).view("I=ii"));
     * TableHandle h2 = ts.execute(TableSpec.emptyTable(42).view("I=ii").where("I % 2 == 0"));
     * </pre>
     *
     * While {@code this} {@link Session} also implements {@link TableServices}, query executions against {@code this}
     * are not cached. In the following example, the second query is re-executed from the beginning.
     *
     * <pre>
     * TableHandle h1 = session.execute(TableSpec.emptyTable(42).view("I=ii"));
     * TableHandle h2 = session.execute(TableSpec.emptyTable(42).view("I=ii").where("I % 2 == 0"));
     * </pre>
     *
     * When using a stateful {@link TableServices}, callers may encounter exceptions that refer to an "unreferenceable
     * table". This is an indication that the caller is trying to export a strict sub-DAG of the existing exports; this
     * is problematic because there isn't (currently) a way to construct a query that guarantees the returned export
     * would refer to the same physical table that the existing exports are based on. The following example demonstrates
     * a case where such an exception would occur.
     *
     * <pre>
     * TableServices ts = session.tableServices();
     * TableHandle h1 = ts.execute(TableSpec.emptyTable(42).view("I=ii").where("I % 2 == 0"));
     * // This execution will throw an "unreferenceable table" exception.
     * TableHandle h2 = ts.execute(TableSpec.emptyTable(42).view("I=ii"));
     * </pre>
     * 
     * @return a new stateful table services
     * @see <a href="https://github.com/deephaven/deephaven-core/issues/4733">deephaven-core#4733</a>
     */
    TableServices tableServices();

    /**
     * Creates a new stateful batch {@link TableHandleManager} that keeps references to the exports created from said
     * service for executing queries with maximum cacheability. See documentation on {@link #tableServices()} for more
     * context on cacheability.
     *
     * @return a new stateful batch table handle manager
     */
    @Override
    TableHandleManager batch();

    /**
     * Creates a new stateful batch {@link TableHandleManager} that keeps references to the exports created from said
     * service for executing queries with maximum cacheability. See documentation on {@link #tableServices()} for more
     * context on cacheability.
     *
     * @param mixinStacktraces if stacktraces should be mixin
     * @return a new stateful batch table handle manager
     */
    @Override
    TableHandleManager batch(boolean mixinStacktraces);

    /**
     * Creates a new stateful serial {@link TableHandleManager} that keeps references to the exports created from said
     * service for executing queries with maximum cacheability. See documentation on {@link #tableServices()} for more
     * context on cacheability.
     *
     * @return a new stateful serial table handle manager
     */
    @Override
    TableHandleManager serial();

    // ----------------------------------------------------------

    /**
     * The authenticated channel.
     *
     * @return the authenticated channel
     */
    DeephavenChannel channel();
}
