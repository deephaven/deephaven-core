package io.deephaven.client.examples;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.client.impl.*;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.InstrumentedShiftAwareListener;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.grpc_api.barrage.BarrageClientSubscription;
import io.deephaven.grpc_api.barrage.BarrageStreamReader;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.FlightExportTicketHelper;
import io.deephaven.grpc_api_client.table.BarrageTable;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * This class is here as a shim between the Java-API and the gRPC Barrage client.  This class will be superseded once
 * proper support for retrieving Barrage tables directly is implemented, in the near future
 */
public class BarrageSupport {
    private static final Logger log = LoggerFactory.getLogger(BarrageSupport.class);

    private final ManagedChannel channel;
    private final FlightServiceGrpc.FlightServiceStub flightService;
    private final FlightSession session;

    // This should really hold WeakReferences to BarrageTable os we can automatically clean up subscriptions.
    // That is out of scope for this example.
    private final Map<BarrageTable, BarrageClientSubscription> subscriptionMap = new HashMap<>();

    public BarrageSupport(final @NotNull ManagedChannel channel, FlightSession session) {
        this.channel = channel;
        this.session = session;
        flightService = FlightServiceGrpc.newStub(channel).withCallCredentials(new SessionCallCredentials());
    }

    /**
     * Fetch a properly subscribed BarrageTable for further use.  All tables retrieved in this manner must be cleaned up
     * by a call to {@link #releaseTable(BarrageTable)} to ensure all server side resources are cleaned up.
     *
     * @param tableName the name of the binding table to fetch
     * @return a subscribed {@link BarrageTable}
     * @throws UncheckedDeephavenException if an error occurs during retrieval or subscription.
     */
    public BarrageTable fetchSubscribedTable(final @NotNull String tableName) throws UncheckedDeephavenException {
        final TableHandle handle = session.session().ticket(tableName);
        final Export tableExport = handle.export();
        final Ticket tableTicket = tableExport.ticket();

        final CompletableFuture<BarrageTable> tableFuture = new CompletableFuture<>();
        flightService.getSchema(
                FlightExportTicketHelper.ticketToDescriptor(tableTicket, "exportTable"),
                new ResponseBuilder<Flight.SchemaResult>()
                        .onError(t -> {
                            log.error().append("Unexpected error ").append(t).endl();
                            tableFuture.completeExceptionally(t);
                        })
                        .onNext(r -> onSchemaResult(r, tableTicket, tableFuture))
                        .build());

        try {
            return tableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new UncheckedDeephavenException("Error retrieving Subscribed Barrage table", e);
        }
    }

    /**
     * Release resources and unsubscribe from a {@link BarrageTable table} retrieved via a call to {@link #fetchSubscribedTable(String)}.
     * @param table the table to release.
     */
    public void releaseTable(final @NotNull BarrageTable table) {
        final BarrageClientSubscription sub = subscriptionMap.get(table);
        if(sub == null) {
            throw new UncheckedDeephavenException("Table was already unsubscribed, or could not be found");
        }

        sub.close();
    }

    /**
     * Close and release -all- subscriptions held by this instance.
     */
    public void close() {
        for(BarrageClientSubscription sub : subscriptionMap.values()) {
            sub.close();
        }
    }

    private void onSchemaResult(final Flight.SchemaResult schemaResult, final Ticket tableTicket, final CompletableFuture<BarrageTable> resultFuture) {
        final Schema schema = Schema.getRootAsSchema(schemaResult.getSchema().asReadOnlyByteBuffer());
        final TableDefinition definition = BarrageSchemaUtil.schemaToTableDefinition(schema);

        final BitSet columns = new BitSet();
        columns.set(0, definition.getColumns().length);

        final BarrageTable resultTable = BarrageTable.make(definition, false);
        final InstrumentedShiftAwareListener listener = new InstrumentedShiftAwareListener("test") {
            @Override
            protected void onFailureInternal(final Throwable originalException,
                                             final UpdatePerformanceTracker.Entry sourceEntry) {
                log.error().append("Unexpected error ").append(originalException).endl();
            }

            @Override
            public void onUpdate(final Update update) {
                log.info().append("received update: ").append(update).endl();
            }
        };
        resultTable.listenForUpdates(listener);

        final BarrageClientSubscription resultSub = new BarrageClientSubscription(
                ExportTicketHelper.toReadableString(tableTicket, "exportTable"),
                channel, BarrageClientSubscription.makeRequest(null, columns),
                new BarrageStreamReader(), resultTable);

        synchronized (subscriptionMap) {
            subscriptionMap.put(resultTable, resultSub);
        }

        resultFuture.complete(resultTable);
    }

    private class SessionCallCredentials extends CallCredentials {
        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor,
                                         MetadataApplier applier) {
            AuthenticationInfo localAuth = ((SessionImpl)session.session()).auth();
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of(localAuth.sessionHeaderKey(), Metadata.ASCII_STRING_MARSHALLER),
                    localAuth.session());
            applier.apply(metadata);
        }

        @Override
        public void thisUsesUnstableApi() {

        }
    }

    private static class ResponseBuilder<T> {
        private Consumer<T> _onNext;
        private Consumer<Throwable> _onError;
        private Runnable _onComplete;

        public ResponseBuilder<T> onNext(final Consumer<T> _onNext) {
            this._onNext = _onNext;
            return this;
        }

        public ResponseBuilder<T> onError(final Consumer<Throwable> _onError) {
            this._onError = _onError;
            return this;
        }

        public ResponseBuilder<T> onComplete(final Runnable _onComplete) {
            this._onComplete = _onComplete;
            return this;
        }

        public StreamObserver<T> build() {
            return new StreamObserver<T>() {
                @Override
                public void onNext(final T value) {
                    if (_onNext != null)
                        _onNext.accept(value);
                }

                @Override
                public void onError(final Throwable t) {
                    if (_onError != null)
                        _onError.accept(t);
                }

                @Override
                public void onCompleted() {
                    if (_onComplete != null)
                        _onComplete.run();
                }
            };
        }
    }
}
