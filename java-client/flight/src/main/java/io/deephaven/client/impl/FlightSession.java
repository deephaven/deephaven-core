package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.grpc_api.util.FlightExportTicketHelper;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TicketTable;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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
     * Perform a GetSchema to get the schema.
     *
     * @param ticket the ticket
     * @return the schema
     */
    public Schema schema(HasTicket ticket) {
        return client.getSchema(descriptor(ticket)).getSchema();
    }

    /**
     * Perform a DoGet to fetch the data.
     *
     * @param ticket the ticket
     * @return the stream
     */
    public FlightStream stream(HasTicket ticket) {
        return client.getStream(ticket(ticket));
    }

    /**
     * Creates a new server side table, backed by the server semantics of DoPut, and returns an appropriate
     * {@link TableHandle handle}.
     *
     * <p>
     * For more advanced use cases, callers may use {@link #putTicket(NewTable, BufferAllocator)}.
     *
     * @param table the table
     * @param allocator the allocator
     * @return the table handle
     * @throws TableHandleException if a handle exception occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    public TableHandle put(NewTable table, BufferAllocator allocator)
            throws TableHandleException, InterruptedException {
        final io.deephaven.proto.backplane.grpc.Ticket ticket = putTicket(table, allocator);
        try {
            // By re-binding from the ticket via TicketTable, we are bringing the doPut table into the proper management
            // structure offered by session.
            return session.execute(TicketTable.of(ticket.getTicket().toByteArray()));
        } finally {
            // We close our raw ticket, since our reference to it will be properly managed by the session now
            release(ticket);
        }
    }

    /**
     * Creates a new server side table, backed by the server semantics of DoPut, and returns an appropriate
     * {@link TableHandle handle}.
     *
     * <p>
     * For more advanced use cases, callers may use {@link #putTicket(FlightStream)}.
     *
     * @param input the input
     * @return the table handle
     * @throws TableHandleException if a handle exception occurs
     * @throws InterruptedException if the current thread is interrupted
     */
    public TableHandle put(FlightStream input) throws TableHandleException, InterruptedException {
        final io.deephaven.proto.backplane.grpc.Ticket ticket = putTicket(input);
        try {
            // By re-binding from the ticket via TicketTable, we are bringing the doPut table into the proper management
            // structure offered by session.
            return session.execute(TicketTable.of(ticket.getTicket().toByteArray()));
        } finally {
            // We close our raw ticket, since our reference to it will be properly managed by the session now
            release(ticket);
        }
    }

    /**
     * Creates a new server side table, backed by the server semantics of DoPut, and returns the low-level
     * {@link io.deephaven.proto.backplane.grpc.Ticket}. Callers are responsible for calling
     * {@link #release(io.deephaven.proto.backplane.grpc.Ticket)}.
     *
     * <p>
     * This method may be more efficient, depending on how the ticket is going to be used. If it will simply be bound to
     * a ticket table, callers should prefer {@link #put(NewTable, BufferAllocator)}.
     *
     * @param table the table
     * @param allocator the allocator
     * @return the ticket
     */
    public io.deephaven.proto.backplane.grpc.Ticket putTicket(NewTable table, BufferAllocator allocator) {
        final io.deephaven.proto.backplane.grpc.Ticket newTicket = session.newTicket();
        final VectorSchemaRoot root = VectorSchemaRootAdapter.of(table, allocator);
        final ClientStreamListener out =
                client.startPut(descriptor(newTicket), root, new AsyncPutListener());
        try {
            out.putNext();
            root.clear();
            out.completed();
            out.getResult();
            return newTicket;
        } catch (Throwable t) {
            session.release(newTicket);
            throw t;
        }
    }

    /**
     * Creates a new server side table, backed by the server semantics of DoPut, and returns the low-level
     * {@link io.deephaven.proto.backplane.grpc.Ticket}. Callers are responsible for calling
     * {@link #release(io.deephaven.proto.backplane.grpc.Ticket)}.
     *
     * <p>
     * This method may be more efficient, depending on how the ticket is going to be used. If it will simply be bound to
     * a ticket table, callers should prefer {@link #put(FlightStream)}.
     *
     * @param input the input
     * @return the ticket
     */
    public io.deephaven.proto.backplane.grpc.Ticket putTicket(FlightStream input) {
        final io.deephaven.proto.backplane.grpc.Ticket newTicket = session.newTicket();
        final ClientStreamListener out =
                client.startPut(descriptor(newTicket), input.getRoot(), new AsyncPutListener());
        try {
            while (input.next()) {
                out.putNext();
                input.getRoot().clear();
            }
            out.completed();
            out.getResult();
            return newTicket;
        } catch (Throwable t) {
            session.release(newTicket);
            throw t;
        }
    }

    /**
     * Releases the low-level {@code ticket}.
     *
     * <p>
     * Note: this should <b>only</b> be called in combination with tickets returned from
     * {@link #putTicket(NewTable, BufferAllocator)} or {@link #putTicket(FlightStream)}.
     *
     * @param ticket the ticket
     * @return the future
     */
    public CompletableFuture<Void> release(io.deephaven.proto.backplane.grpc.Ticket ticket) {
        return session.release(ticket);
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

    private static FlightDescriptor descriptor(HasTicket ticket) {
        return descriptor(ticket.ticket());
    }

    private static FlightDescriptor descriptor(io.deephaven.proto.backplane.grpc.Ticket ticket) {
        return descriptor(FlightExportTicketHelper.ticketToDescriptor(ticket, "export"));
    }

    private static FlightDescriptor descriptor(org.apache.arrow.flight.impl.Flight.FlightDescriptor impl) {
        switch (impl.getType()) {
            case PATH:
                return FlightDescriptor.path(impl.getPathList());
            case CMD:
                return FlightDescriptor.command(impl.getCmd().toByteArray());
            default:
                throw new IllegalArgumentException("Unexpected type " + impl.getTypeValue());
        }
    }

    private static Ticket ticket(HasTicket ticket) {
        return new Ticket(ticket.ticket().getTicket().toByteArray());
    }
}
