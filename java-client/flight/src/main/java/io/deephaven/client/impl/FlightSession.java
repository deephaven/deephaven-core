package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.qst.table.NewTable;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class FlightSession implements AutoCloseable {

    public static FlightSession of(SessionImpl session, BufferAllocator incomingAllocator,
            ManagedChannel channel) {
        final FlightClient client = FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(
                incomingAllocator, channel, Collections.singletonList(new SessionMiddleware(session)));
        return new FlightSession(session, client);
    }

    protected final SessionImpl session;

    // TODO(deephaven-core#988): Add more async support to org.apache.arrow.flight.FlightClient
    protected final FlightClient client;

    protected FlightSession(SessionImpl session, FlightClient client) {
        this.session = Objects.requireNonNull(session);
        this.client = Objects.requireNonNull(client);
    }

    /**
     * The session.
     *
     * @return the session
     */
    public Session session() {
        return session;
    }

    /**
     * Create a schema from the existing handle's response.
     *
     * <p>
     * Equivalent to {@code SchemaHelper.schema(handle.response())}.
     *
     * @param handle the handle
     * @return the schema
     * @see SchemaHelper#schema(ExportedTableCreationResponse)
     */
    public Schema schema(TableHandle handle) {
        return SchemaHelper.schema(handle.response());
    }

    /**
     * Perform a GetSchema to get the schema.
     *
     * @param pathId the path ID
     * @return the schema
     */
    public Schema schema(HasPathId pathId) {
        return FlightClientHelper.getSchema(client, pathId).getSchema();
    }

    /**
     * Perform a DoGet to fetch the data.
     *
     * @param ticketId the ticket
     * @return the stream
     */
    public FlightStream stream(HasTicketId ticketId) {
        return FlightClientHelper.get(client, ticketId);
    }

    /**
     * Creates a new server side DoExchange session.
     *
     * @param descriptor the FlightDescriptor object to include on the first FlightData message (other fields will
     *        remain null)
     * @param options the GRPC otions to apply to this call
     * @return the bi-directional ReaderWriter object
     */
    public FlightClient.ExchangeReaderWriter startExchange(FlightDescriptor descriptor, CallOption... options) {
        return client.doExchange(descriptor, options);
    }

    /**
     * Creates a new server side exported table backed by the server semantics of DoPut with a {@link NewTable} payload.
     *
     * <p>
     * For more advanced use cases, callers may use {@link #putExportManual(NewTable, BufferAllocator)}.
     *
     * @param table the table
     * @param allocator the allocator
     * @return the table handle
     * @throws TableHandleException if a handle exception occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    public TableHandle putExport(NewTable table, BufferAllocator allocator)
            throws TableHandleException, InterruptedException {
        final ExportId exportId = putExportManual(table, allocator);
        try {
            // By re-binding from the ticket via TicketTable, we are bringing the doPut table into the proper management
            // structure offered by session.
            return session.execute(exportId.ticketId().table());
        } finally {
            // We close our raw ticket, since our reference to it will be properly managed by the session now
            release(exportId);
        }
    }

    /**
     * Creates a new server side exported table backed by the server semantics of DoPut with a {@link FlightStream}
     * payload.
     *
     * <p>
     * For more advanced use cases, callers may use {@link #putExportManual(FlightStream)}.
     *
     * @param input the input
     * @return the table handle
     * @throws TableHandleException if a handle exception occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    public TableHandle putExport(FlightStream input) throws TableHandleException, InterruptedException {
        final ExportId export = putExportManual(input);
        try {
            // By re-binding from the ticket via TicketTable, we are bringing the doPut table into the proper management
            // structure offered by session.
            return session.execute(export.ticketId().table());
        } finally {
            // We close our raw ticket, since our reference to it will be properly managed by the session now
            release(export);
        }
    }

    /**
     * Creates a new server side export table backed by the server semantics for DoPut with a {@link NewTable} payload.
     * Callers are responsible for calling {@link #release(ExportId)}.
     *
     * <p>
     * This method may be more efficient, depending on how the export is going to be used. If it will simply be bound to
     * another export table, callers should prefer {@link #putExport(NewTable, BufferAllocator)}.
     *
     * @param table the table
     * @param allocator the allocator
     * @return the ticket
     */
    public ExportId putExportManual(NewTable table, BufferAllocator allocator) {
        final ExportId exportTicket = session.newExportId();
        try {
            put(exportTicket, table, allocator);
        } catch (Throwable t) {
            session.release(exportTicket);
            throw t;
        }
        return exportTicket;
    }

    /**
     * Creates a new server side export table backed by the server semantics for DoPut with a {@link FlightStream}
     * payload. Callers are responsible for calling {@link #release(ExportId)}.
     *
     * <p>
     * This method may be more efficient, depending on how the ticket is going to be used. If it will simply be bound to
     * a ticket table, callers should prefer {@link #putExport(FlightStream)}.
     *
     * @param input the input
     * @return the export ID
     */
    public ExportId putExportManual(FlightStream input) {
        final ExportId exportTicket = session.newExportId();
        try {
            put(exportTicket, input);
        } catch (Throwable t) {
            session.release(exportTicket);
            throw t;
        }
        return exportTicket;
    }

    /**
     * Performs a DoPut against the {@code pathId} with a {@link FlightStream} payload.
     *
     * @param pathId the path ID
     * @param input the input
     */
    public void put(HasPathId pathId, FlightStream input) {
        FlightClientHelper.put(client, pathId, input);
    }

    /**
     * Performs a DoPut against the {@code pathId} with a {@link NewTable} payload.
     *
     * @param pathId the path ID
     * @param table the table
     * @param allocator the allocator
     */
    public void put(HasPathId pathId, NewTable table, BufferAllocator allocator) {
        FlightClientHelper.put(client, pathId, table, allocator);
    }

    /**
     * Add {@code source} to the input table {@code destination}.
     *
     * @param destination the destination input table
     * @param source the source
     * @return the future
     * @see #putExportManual(FlightStream)
     * @see Session#addToInputTable(HasTicketId, HasTicketId)
     */
    public CompletableFuture<Void> addToInputTable(HasTicketId destination,
            FlightStream source) {
        // TODO: would be nice to implicitly addToInputTable for appropriate doPuts - one call instead of two
        // https://github.com/deephaven/deephaven-core/discussions/1578
        final ExportId exportId = putExportManual(source);
        final CompletableFuture<Void> future = session.addToInputTable(destination, exportId);
        future.whenComplete((result, error) -> release(exportId));
        return future;
    }

    /**
     * Add {@code source} to the input table {@code destination}.
     *
     * @param destination the destination input table
     * @param source the source
     * @return the future
     * @see #putExportManual(NewTable, BufferAllocator)
     * @see Session#addToInputTable(HasTicketId, HasTicketId)
     */
    public CompletableFuture<Void> addToInputTable(HasTicketId destination,
            NewTable source, BufferAllocator allocator) {
        // TODO: would be nice to implicitly addToInputTable for appropriate doPuts - one call instead of two
        // https://github.com/deephaven/deephaven-core/discussions/1578
        final ExportId exportId = putExportManual(source, allocator);
        final CompletableFuture<Void> future = session.addToInputTable(destination, exportId);
        future.whenComplete((result, error) -> release(exportId));
        return future;
    }

    /**
     * Delete {@code source} from the input table {@code destination}.
     *
     * @param destination the destination input table
     * @param source the source
     * @return the future
     * @see #putExportManual(FlightStream)
     * @see Session#deleteFromInputTable(HasTicketId, HasTicketId)
     */
    public CompletableFuture<Void> deleteFromInputTable(HasTicketId destination,
            FlightStream source) {
        // TODO: would be nice to implicitly addToInputTable for appropriate doPuts - one call instead of two
        // https://github.com/deephaven/deephaven-core/discussions/1578
        final ExportId exportId = putExportManual(source);
        final CompletableFuture<Void> future = session.deleteFromInputTable(destination, exportId);
        future.whenComplete((result, error) -> release(exportId));
        return future;
    }

    /**
     * Delete {@code source} from the input table {@code destination}.
     *
     * @param destination the destination input table
     * @param source the source
     * @return the future
     * @see #putExportManual(NewTable, BufferAllocator)
     * @see Session#deleteFromInputTable(HasTicketId, HasTicketId)
     */
    public CompletableFuture<Void> deleteFromInputTable(HasTicketId destination,
            NewTable source,
            BufferAllocator allocator) {
        // TODO: would be nice to implicitly addToInputTable for appropriate doPuts - one call instead of two
        // https://github.com/deephaven/deephaven-core/discussions/1578
        final ExportId exportId = putExportManual(source, allocator);
        final CompletableFuture<Void> future = session.deleteFromInputTable(destination, exportId);
        future.whenComplete((result, error) -> release(exportId));
        return future;
    }

    /**
     * Releases the {@code exportId}.
     *
     * <p>
     * Note: this should <b>only</b> be called in combination with export IDs returned from
     * {@link #putExportManual(NewTable, BufferAllocator)} or {@link #putExportManual(FlightStream)}.
     *
     * @param exportId the export ID
     * @return the future
     */
    public CompletableFuture<Void> release(ExportId exportId) {
        return session.release(exportId);
    }

    /**
     * List the flights.
     *
     * @return the flights
     */
    public Iterable<FlightInfo> list() {
        return client.listFlights(Criteria.ALL);
    }

    @Override
    public void close() throws InterruptedException {
        client.close();
    }
}
