package io.deephaven.web.client.api.batch;

import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import elemental2.promise.Promise.PromiseExecutorCallbackFn.RejectCallbackFn;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.BatchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.batch.BatchBuilder.BatchOp;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.state.ActiveTableBinding;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.CustomColumnDescriptor;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.MappedIterable;
import jsinterop.annotations.JsMethod;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * A bucket for queuing up requests on Tables to be sent all at once.
 *
 * Currently scoped to a single table, but we should be able to refactor this to handle multiple tables at once (by
 * pushing table/handles into method signatures)
 *
 * TODO fix core#80
 */
public class RequestBatcher {

    private final JsTable table;
    private final BatchBuilder builder;
    final List<ClientTableState> orphans;
    private final List<JsConsumer<BatchTableRequest>> onSend;
    private final WorkerConnection connection;
    private boolean sent;
    private boolean finished;
    private boolean failed;

    public RequestBatcher(JsTable table, WorkerConnection connection) {
        this.table = table;
        this.connection = connection;
        builder = new BatchBuilder();
        onSend = new ArrayList<>();
        orphans = new ArrayList<>();
    }

    public BatchTableRequest buildRequest() {
        createOps();
        final BatchTableRequest request = new BatchTableRequest();
        request.setOpsList(builder.serializable());
        return request;
    }

    public Promise<JsTable> nestedPromise(JsTable table) {
        createOps();
        final ClientTableState state = builder.getOp().getState();
        return new Promise<>(((resolve, reject) -> {
            state.onRunning(ignored -> resolve.onInvoke(table), reject::onInvoke,
                    () -> reject.onInvoke("Table failed, or was closed"));
        }));
    }

    private void createOps() {
        try {
            doCreateOps();
        } catch (Throwable t) {
            sent = true;
            finished = true;
            failed = true;
            throw t;
        }
    }

    private ActiveTableBinding rollbackTo;

    private void doCreateOps() {
        BatchOp op = builder.getOp();
        ClientTableState appendTo = table.state();
        assert appendTo.getBinding(table).isActive()
                : "Table state " + appendTo + " did not have " + table + " actively bound to it";
        rollbackTo = appendTo.getActiveBinding(table);

        while (!appendTo.isCompatible(op.getSorts(), op.getFilters(), op.getCustomColumns(), op.isFlat())) {
            final ClientTableState nextTry = appendTo.getPrevious();
            assert nextTry != null : "Root state " + appendTo + " is not blank!";
            orphans.add(appendTo);
            // pause the orphan state right away for our main table
            appendTo.pause(table);
            appendTo = nextTry;
        }

        // Set the table to the given state.
        table.setState(appendTo);
        if (op.isEqual(appendTo) && appendTo.isRunning()) {
            // The node we want to become next is already running.
            op.setState(appendTo);
            op.setAppendTo(appendTo.getPrevious());
            // we didn't set any handle; this tells other code to not bother to send the op,
            // but we'll still keep it around to process as an active state
            builder.doNextOp(op);
        } else {
            // Create new states. If we are adding both filters and sorts, we'll want an intermediate table
            final BatchOp newOp = maybeInsertInterimTable(op, appendTo);

            // Insert the final operation
            insertOp(newOp, table.state());
        }
    }

    private ClientTableState insertOp(BatchOp op, ClientTableState sourceState) {

        assert table.state() == sourceState : "You must update your table's currentState before calling insertOp";
        final TableTicket handle = sourceState.getHandle();
        final ClientTableState newTail = connection.newState(sourceState, op);
        table.setState(newTail);
        table.setRollback(rollbackTo);
        op.setState(newTail);
        op.setHandles(handle, newTail.getHandle());
        // add the intermediate state to the request, causes next call to getOp() to return a new builder
        builder.doNextOp(op);

        return newTail;
    }

    private MappedIterable<ClientTableState> allStates() {
        return builder.getOps().mapped(BatchOp::getState).filterNonNull();
    }

    private MappedIterable<JsTable> allInterestedTables() {
        IdentityHashMap<JsTable, JsTable> tables = new IdentityHashMap<>();
        if (!table.isAlive()) {
            tables.put(table, table);
        }
        for (ClientTableState state : allStates()) {
            state.getBoundTables().forEach(table -> {
                if (!tables.containsKey(table)) {
                    // now, lets make sure this client still cares about the given state...
                    // (a table may be holding a paused binding to this state)
                    if (table.hasHandle(state.getHandle())) {
                        tables.put(table, table);
                    }
                }
            });
        }
        return tables.keySet()::iterator;
    }

    private BatchOp maybeInsertInterimTable(BatchOp op, ClientTableState appendTo) {
        op.setAppendTo(appendTo);
        boolean filterChanged = !appendTo.getFilters().equals(op.getFilters());
        if (filterChanged) {
            // whenever filters have changed, we will create one exported table w/ filters and any custom columns.
            // if there are _also_ sorts that have changed, then we'll want to add those on afterwards.
            final List<Sort> appendToSort = appendTo.getSorts();
            final List<Sort> desiredSorts = op.getSorts();
            boolean sortChanged = !appendToSort.equals(desiredSorts);
            if (sortChanged) {
                // create an intermediate state with just custom columns and filters.
                op.setSorts(appendToSort);
                final ClientTableState newAppendTo = insertOp(op, appendTo);

                // get a new "next state" to build, which should only cause sorts to be added.
                final BatchOp newOp = builder.getOp();
                newOp.setSorts(desiredSorts);
                newOp.setFilters(op.getFilters());
                newOp.setCustomColumns(op.getCustomColumns());
                newOp.setFlat(op.isFlat());
                newOp.setAppendTo(newAppendTo);
                JsLog.debug("Adding interim operation to batch", op, " -> ", newOp);
                return newOp;
            }
        }
        return op;
    }

    public Promise<Void> sendRequest() {
        return new Promise<>((resolve, reject) -> {

            // calling buildRequest will change the state of each table, so we first check what the state was
            // of each table before we do that, in case we are actually not making a call to the server
            ClientTableState prevState = table.lastVisibleState();

            final BatchTableRequest request = buildRequest();

            // let callbacks peek at entire request about to be sent.
            for (JsConsumer<BatchTableRequest> send : onSend) {
                send.apply(request);
            }
            onSend.clear();
            sent = true;
            if (request.getOpsList().length == 0) {
                // Since this is an empty request, there are no "interested" tables as we normally would define them,
                // so we can only operate on the root table object
                // No server call needed - we need to examine the "before" state and fire events based on that.
                // This is like tableLoop below, except no failure is possible, since we already have the results
                if (table.isAlive()) {
                    final ClientTableState active = table.state();
                    assert active.isRunning() : active;
                    boolean sortChanged = !prevState.getSorts().equals(active.getSorts());
                    boolean filterChanged = !prevState.getFilters().equals(active.getFilters());
                    boolean customColumnChanged = !prevState.getCustomColumns().equals(active.getCustomColumns());
                    table.fireEvent(HasEventHandling.EVENT_REQUEST_SUCCEEDED);
                    // TODO think more about the order of events, and what kinds of things one might bind to each
                    if (sortChanged) {
                        table.fireEvent(JsTable.EVENT_SORTCHANGED);
                    }
                    if (filterChanged) {
                        table.fireEvent(JsTable.EVENT_FILTERCHANGED);
                    }
                    if (customColumnChanged) {
                        table.fireEvent(JsTable.EVENT_CUSTOMCOLUMNSCHANGED);
                    }
                }

                orphans.forEach(ClientTableState::cleanup);
                resolve.onInvoke((Void) null);
                // if there's no outgoing operation, user may have removed an operation and wants to return to where
                // they left off
                table.maybeReviveSubscription();
                finished = true;
                return;
            }

            final BatchOp operationHead = builder.getFirstOp();
            assert operationHead != null : "A non-empty request must have a firstOp!";
            final ClientTableState source = operationHead.getAppendTo();
            assert source != null : "A non-empty request must have a source state!";

            JsLog.debug("Sending request", LazyString.of(request), request, " based on ", this);


            ResponseStreamWrapper<ExportedTableCreationResponse> batchStream =
                    ResponseStreamWrapper.of(connection.tableServiceClient().batch(request, connection.metadata()));
            batchStream.onData(response -> {
                TableReference resultid = response.getResultId();
                if (!resultid.hasTicket()) {
                    // thanks for telling us, but we don't at this time have a nice way to indicate this
                    return;
                }
                Ticket ticket = resultid.getTicket();
                if (!response.getSuccess()) {
                    String fail = response.getErrorInfo();

                    // any table which has that state active should fire a failed event
                    ClientTableState state = allStates().filter(
                            cts -> cts.getHandle().makeTicket().getTicket_asB64().equals(ticket.getTicket_asB64()))
                            .first();
                    state.getHandle().setState(TableTicket.State.FAILED);
                    for (JsTable table : allInterestedTables().filter(t -> t.state() == state)) {
                        // fire the failed event
                        failTable(table, fail);
                    }

                    // mark state as failed (his has to happen after table is marked as failed, it will trigger rollback
                    state.setResolution(ClientTableState.ResolutionState.FAILED, fail);

                    return;
                }

                // any table which has that state active should fire a failed event
                ClientTableState state = allStates()
                        .filter(cts -> cts.getHandle().makeTicket().getTicket_asB64().equals(ticket.getTicket_asB64()))
                        .first();
                // state.getHandle().setState(TableTicket.State.EXPORTED);
                for (JsTable table : allInterestedTables().filter(t -> t.state() == state)) {
                    // check what state it was in previously to use for firing an event
                    ClientTableState lastVisibleState = table.lastVisibleState();

                    // mark the table as ready to go
                    state.applyTableCreationResponse(response);
                    state.forActiveTables(t -> t.maybeRevive(state));
                    state.setResolution(ClientTableState.ResolutionState.RUNNING);

                    // fire any events that are necessary
                    boolean sortChanged = !lastVisibleState.getSorts().equals(state.getSorts());
                    boolean filterChanged = !lastVisibleState.getFilters().equals(state.getFilters());
                    boolean customColumnChanged = !lastVisibleState.getCustomColumns().equals(state.getCustomColumns());
                    table.fireEvent(HasEventHandling.EVENT_REQUEST_SUCCEEDED);
                    // TODO think more about the order of events, and what kinds of things one might bind to each
                    if (sortChanged) {
                        table.fireEvent(JsTable.EVENT_SORTCHANGED);
                    }
                    if (filterChanged) {
                        table.fireEvent(JsTable.EVENT_FILTERCHANGED);
                    }
                    if (customColumnChanged) {
                        table.fireEvent(JsTable.EVENT_CUSTOMCOLUMNSCHANGED);
                    }
                }

            });

            batchStream.onEnd(status -> {
                // request is complete
                if (status.isOk()) {
                    resolve.onInvoke((Void) null);
                } else {
                    failed(reject, status.getDetails());
                }
            });
        });
    }

    private void failTable(JsTable t, String failureMessage) {
        final CustomEventInit event = CustomEventInit.create();
        ClientTableState best = t.state();
        for (ClientTableState state : best.reversed()) {
            if (allStates().anyMatch(state::equals)) {
                best = state;
                break;
            }
        }

        event.setDetail(JsPropertyMap.of(
                "errorMessage", failureMessage,
                "configuration", best.toJs()));
        try {
            t.rollback();
        } catch (Exception e) {
            JsLog.warn(
                    "An exception occurred trying to rollback the table. This means that there will be no ticking data until the table configuration is applied again in a way that makes sense. See IDS-5199 for more detail.",
                    e);
        }
        t.fireEvent(HasEventHandling.EVENT_REQUEST_FAILED, event);
    }

    private void failed(RejectCallbackFn reject, String fail) {
        failed = true;
        for (ClientTableState s : allStates().filter(cts -> !cts.isFinished())) {
            s.setResolution(ClientTableState.ResolutionState.FAILED, fail);
        }
        for (JsTable t : allInterestedTables()) {
            failTable(t, fail);
        }
        reject.onInvoke(fail);
        // any batches that depend on us must also be failed / cancelled...
    }

    @JsMethod
    public void setSort(Sort[] newSort) {
        builder.setSort(Arrays.asList(newSort));
    }

    public void sort(List<Sort> newSort) {
        builder.setSort(newSort);
    }

    @JsMethod
    public void setFilter(FilterCondition[] newFilter) {
        builder.setFilter(Arrays.asList(newFilter));
    }

    public void filter(List<FilterCondition> newFilter) {
        builder.setFilter(newFilter);
    }

    @JsMethod
    public void setCustomColumns(String[] newColumns) {
        builder.setCustomColumns(CustomColumnDescriptor.from(newColumns));
    }

    public void customColumns(List<CustomColumnDescriptor> newColumns) {
        builder.setCustomColumns(newColumns);
    }

    @JsMethod
    public void setFlat(boolean isFlat) {
        builder.setFlat(isFlat);
    }

    public void onSend(JsConsumer<BatchTableRequest> success) {
        onSend.add(success);
    }

    public boolean isSent() {
        return sent;
    }

    public boolean isFinished() {
        return finished;
    }

    public boolean isFailed() {
        return failed;
    }

    public final boolean isInProgress() {
        return isSent() && !isFinished();
    }

    /**
     * If there is a pending operation, finish it
     */
    public void finishOp() {
        createOps();
    }

    public void setConfig(TableConfig other) {
        customColumns(other.getCustomColumns());
        sort(other.getSorts());
        filter(other.getFilters());
        // selectDistinct ignored as it's not really treated as a standard table operation right now.
        setFlat(other.isFlat());
    }
}
