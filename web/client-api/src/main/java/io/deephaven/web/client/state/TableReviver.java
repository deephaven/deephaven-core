//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.state;

import elemental2.core.JsArray;
import elemental2.core.JsMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.BatchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.batchtablerequest.Operation;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.batch.BatchBuilder;
import io.deephaven.web.client.api.batch.BatchBuilder.BatchOp;
import io.deephaven.web.client.api.batch.RequestBatcher;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.fu.IdentityHashSet;

import java.util.*;

/**
 * Instances of this class are responsible for bringing CTS back to life.
 *
 * The {@link RequestBatcher} class has been refactored to take an interface, {@link HasTableBinding}, which the
 * TableReviver implements, so that it can assemble "rebuild this state" requests.
 *
 */
public class TableReviver implements HasTableBinding {

    private final JsMap<TableTicket, ClientTableState> targets;
    private final WorkerConnection connection;
    private ClientTableState currentState;
    private Set<ClientTableState> enqueued;

    public TableReviver(WorkerConnection connection) {
        targets = new JsMap<>();
        this.connection = connection;
    }

    public void revive(BrowserHeaders metadata, ClientTableState... states) {
        if (enqueued == null) {
            enqueued = new IdentityHashSet<>();
            LazyPromise.runLater(() -> {
                final ClientTableState[] toRevive = enqueued.stream()
                        .filter(ClientTableState::shouldResuscitate)
                        .toArray(ClientTableState[]::new);
                enqueued = null;
                doRevive(toRevive, metadata);
            });
        }
        enqueued.addAll(Arrays.asList(states));
    }

    private void doRevive(ClientTableState[] states, BrowserHeaders metadata) {

        // We want the server to start as soon as possible,
        // so we'll separate "root table fetches" and send them first.
        List<ClientTableState> reviveFirst = new ArrayList<>();
        // All derived table states can get sent in batch after the root items.
        List<ClientTableState> reviveLast = new ArrayList<>();

        for (ClientTableState state : states) {
            // TODO (deephaven-core#3501) for new session creation, we may want to maintain two lists instead
            reviveFirst.add(state);
            // (state.getPrevious() == null ? reviveFirst : reviveLast).add(state);
        }
        JsLog.debug("Reviving states; roots:", reviveFirst, "leaves:", reviveLast);
        // MRU-ordered revivification preferences.
        final Comparator<? super ClientTableState> newestFirst = ClientTableState.newestFirst();
        reviveFirst.sort(newestFirst);
        reviveLast.sort(newestFirst);

        for (ClientTableState state : reviveFirst) {
            JsLog.debug("Attempting revive on ", state);
            state.maybeRevive(metadata).then(
                    success -> {
                        state.forActiveLifecycles(t -> ((JsTable) t).revive(state));
                        return null;
                    }, failure -> {
                        state.forActiveLifecycles(t -> t.die(failure));
                        return null;
                    });
        }

        if (!reviveLast.isEmpty()) {
            // Instead of using RequestBatcher, we should just be rebuilding the SerializedTableOps directly.
            int cnt = 0, page = 6;
            BatchBuilder builder = new BatchBuilder();
            Map<TableTicket, ClientTableState> all = new LinkedHashMap<>();
            for (ClientTableState s : reviveLast) {
                all.put(s.getHandle(), s);
                final BatchOp rebuild = builder.getOp();
                rebuild.fromState(s);
                rebuild.setAppendTo(s.getPrevious());
                rebuild.setHandles(s.getPrevious().getHandle(), s.getHandle());
                builder.doNextOp(rebuild);
                if (++cnt == page) {
                    cnt = 0;
                    page += 4;
                    sendRequest(builder, all);
                    all = new LinkedHashMap<>();
                }
            }
            sendRequest(builder, all);
        }
    }

    private void sendRequest(BatchBuilder requester, Map<TableTicket, ClientTableState> all) {
        final JsArray<Operation> ops = requester.serializable();
        if (ops.length == 0) {
            return;
        }
        final BatchTableRequest req = new BatchTableRequest();
        req.setOpsList(ops);
        requester.clear();
        JsLog.debug("Sending revivification request", LazyString.of(req));

        // TODO core#242 - this isn't tested at all, and mostly doesn't make sense
        ResponseStreamWrapper<ExportedTableCreationResponse> stream =
                ResponseStreamWrapper.of(connection.tableServiceClient().batch(req, connection.metadata()));
        stream.onData(response -> {
            TableReference resultid = response.getResultId();
            if (!resultid.hasTicket()) {
                // thanks for telling us, but we don't at this time have a nice way to indicate this
                return;
            }
            Ticket ticket = resultid.getTicket();

            if (!response.getSuccess()) {
                ClientTableState dead = all.remove(new TableTicket(ticket.getTicket_asU8()));
                dead.forActiveLifecycles(t -> t.die(response.getErrorInfo()));
            } else {
                ClientTableState succeeded = all.remove(new TableTicket(ticket.getTicket_asU8()));
                succeeded.setResolution(ClientTableState.ResolutionState.RUNNING);
                succeeded.forActiveLifecycles(t -> ((JsTable) t).revive(succeeded));
            }
        });
        stream.onEnd(status -> {
            if (status.isOk()) {
                for (ClientTableState failed : all.values()) {
                    failed.forActiveLifecycles(t -> t.die(status.getDetails()));
                }
            }
        });
    }

    @Override
    public TableTicket getHandle() {
        return currentState.getHandle();
    }

    @Override
    public boolean hasHandle(TableTicket tableHandle) {
        return targets.has(tableHandle);
    }

    @Override
    public ClientTableState state() {
        return currentState;
    }

    @Override
    public boolean isAlive() {
        return true; // always alive!
    }

    @Override
    public void fireEvent(String name) {
        JsLog.debug("The table reviver does not accept event", name);
    }

    @Override
    public <T> void fireEvent(String name, T detail) {
        switch (name) {
            case JsTable.EVENT_REQUEST_FAILED:
                // log this failure
                JsLog.debug("Revivification failed", detail);
                //
                return;
            default:
                JsLog.debug("The table reviver does not accept event", name, detail);
        }
    }

    @Override
    public void setState(ClientTableState appendTo) {
        this.currentState = appendTo;
    }

    @Override
    public void rollback() {
        assert false : "Revivification requests should not be sent through the RequestBatcher " +
                "(who is, currently, the only caller of rollback())";
    }

    @Override
    public void setRollback(ActiveTableBinding rollbackTo) {
        assert false : "Revivification requests should not be sent through the RequestBatcher " +
                "(who is, currently, the only caller of setRollback())";
    }

    @Override
    public void maybeReviveSubscription() {
        // should never be called
        assert false : "Revivification requests should not be sent through the RequestBatcher " +
                "(who is, currently, the only caller of maybeReviveSubscription())";
    }
}
