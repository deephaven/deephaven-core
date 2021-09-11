package io.deephaven.client.examples;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.client.impl.*;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.grpc_api.barrage.BarrageClientSubscription;
import io.deephaven.grpc_api.barrage.BarrageStreamReader;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api_client.table.BarrageTable;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.grpc.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is here as a shim between the Java-API and the gRPC Barrage client.  This class will be superseded once
 * proper support for retrieving Barrage tables directly is implemented, in the near future
 */
public class BarrageSupport {
    private final Channel channel;
    private final FlightSession session;

    // This should really hold WeakReferences to BarrageTable os we can automatically clean up subscriptions.
    // That is out of scope for this example.
    private final Map<BarrageTable, BarrageClientSubscription> subscriptionMap = new HashMap<>();

    public BarrageSupport(final @NotNull ManagedChannel channel, FlightSession session) {
        this.channel = ClientInterceptors.intercept(channel, new AuthInterceptor());
        this.session = session;
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

        final Schema schema = session.getSchema(tableExport);
        final TableDefinition definition = BarrageSchemaUtil.schemaToTableDefinition(schema);

        final BitSet columns = new BitSet();
        columns.set(0, definition.getColumns().length);

        final BarrageTable resultTable = BarrageTable.make(definition, false);
        final BarrageClientSubscription resultSub = new BarrageClientSubscription(
                ExportTicketHelper.toReadableString(tableTicket, "exportTable"),
                channel, BarrageClientSubscription.makeRequest(tableTicket, null, columns),
                new BarrageStreamReader(), resultTable);

        synchronized (subscriptionMap) {
            subscriptionMap.put(resultTable, resultSub);
        }

        return resultTable;
    }

    /**
     * Release resources and unsubscribe from a {@link BarrageTable table} retrieved via a call to {@link #fetchSubscribedTable(String)}.
     * @param table the table to release.
     */
    public void releaseTable(final @NotNull BarrageTable table) {
        final BarrageClientSubscription sub;
        synchronized (subscriptionMap) {
            sub = subscriptionMap.remove(table);
        }

        if(sub == null) {
            throw new UncheckedDeephavenException("Table was already unsubscribed, or could not be found");
        }

        sub.close();
    }

    /**
     * Close and release -all- subscriptions held by this instance.
     */
    public void close() {
        synchronized (subscriptionMap) {
            for (final BarrageClientSubscription sub : subscriptionMap.values()) {
                sub.close();
            }
        }
    }

    private class AuthInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                final Channel channel) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    channel.newCall(methodDescriptor, callOptions)) {
                @Override
                public void start(final Listener<RespT> responseListener, final Metadata headers) {
                    final AuthenticationInfo localAuth = ((SessionImpl)session.session()).auth();
                    headers.put(Metadata.Key.of(localAuth.sessionHeaderKey(), Metadata.ASCII_STRING_MARSHALLER),
                            localAuth.session());
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
