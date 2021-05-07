package io.deephaven.ide.shared;

import elemental2.core.JsArray;
import elemental2.core.JsSet;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.BindTableToVariableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ExecuteCommandRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ExecuteCommandResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.console.JsCommandResult;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.widget.plot.JsFigure;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.LogItem;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.ide.CommandResult;
import io.deephaven.web.shared.ide.ExecutionHandle;
import io.deephaven.web.shared.ide.VariableChanges;
import io.deephaven.web.shared.ide.VariableDefinition;
import io.deephaven.web.shared.ide.lsp.CompletionItem;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import static io.deephaven.web.client.api.QueryConnectable.EVENT_TABLE_OPENED;

/**
 */
@JsType(namespace = "dh")
public class IdeSession extends HasEventHandling {
    public static final String EVENT_COMMANDSTARTED = "commandstarted";

    private final Ticket result;

    private final JsSet<ExecutionHandle> cancelled;
    private final WorkerConnection connection;
    private final JsRunnable closer;

    @JsIgnore
    public IdeSession(
        WorkerConnection connection,
        Ticket connectionResult,
        JsRunnable closer
    ) {
        this.result = connectionResult;
        cancelled = new JsSet<>();
        this.connection = connection;
        this.closer = closer;
    }

    public Promise<JsTable> getTable(String name) {
        final Promise<JsTable> table = connection.getTable(name, result);
        final CustomEventInit event = CustomEventInit.create();
        event.setDetail(table);
        fireEvent(EVENT_TABLE_OPENED, event);
        return table;
    }

    public Promise<JsTreeTable> getTreeTable(String name) {
        return connection.getTreeTable(name, result);
    }

    public Promise<JsFigure> getFigure(String name) {
        return connection.getFigure(name, result);
    }

    public Promise<Object> getObject(Object definitionObject) {
        JsVariableDefinition definition = JsVariableDefinition.from(definitionObject);
        return connection.getObject(definition, result);
    }

    public Promise<JsTable> newTable(String[] columnNames, String[] types, String[][] data, String userTimeZone) {
        return connection.newTable(columnNames, types, data, userTimeZone, this).then(table -> {
            final CustomEventInit event = CustomEventInit.create();
            event.setDetail(table);
            fireEvent(EVENT_TABLE_OPENED, event);

            return Promise.resolve(table);
        });
    }

    public Promise<JsTable> mergeTables(JsTable[] tables) {
        return connection.mergeTables(tables, this).then(table -> {
            final CustomEventInit event = CustomEventInit.create();
            event.setDetail(table);
            fireEvent(EVENT_TABLE_OPENED, event);

            return Promise.resolve(table);
        });
    }

    public Promise<Void> bindTableToVariable(JsTable table, String name) {
        BindTableToVariableRequest bindRequest = new BindTableToVariableRequest();
        bindRequest.setConsoleid(table.getHandle().makeTicket());
        bindRequest.setVariablename(name);
        return Callbacks.grpcUnaryPromise(c ->
                connection.consoleServiceClient().bindTableToVariable(bindRequest, connection.metadata(), c::apply))
                .then(ignore -> Promise.resolve((Void)null)
        );
    }

    public void close() {
        closer.run();
    }

    public CancellablePromise<JsCommandResult> runCode(String code) {
        LazyPromise<CommandResult> promise = new LazyPromise<>();

        ExecuteCommandRequest request = new ExecuteCommandRequest();
        request.setConsoleid(this.result);
        request.setCode(code);
        Promise<ExecuteCommandResponse> runCodePromise = Callbacks.grpcUnaryPromise(c -> {
            connection.consoleServiceClient().executeCommand(request, connection.metadata(), c::apply);
        });
        runCodePromise.then(response -> {
            CommandResult commandResult = new CommandResult();
            commandResult.setError(response.getErrormessage());
            VariableChanges changes = new VariableChanges();
            changes.created = copyVariables(response.getCreatedList());
            changes.updated = copyVariables(response.getUpdatedList());
            changes.removed = copyVariables(response.getRemovedList());
            commandResult.setChanges(changes);
            promise.succeed(commandResult);
            return null;
        }, err -> {
            promise.fail(err);
            return null;
        });

        CancellablePromise<JsCommandResult> result = promise.asPromise(
                res -> new JsCommandResult(res),
                () -> {
//                    cancelled.add(handle);
//                    CancelCommandRequest cancelRequest = new CancelCommandRequest();
//                    cancelRequest.setCommandid();
//                    connection.consoleServiceClient().cancelCommand(cancelRequest, connection.metadata());
                    throw new UnsupportedOperationException("cancelCommand");
                }
        );

        CommandInfo commandInfo = new CommandInfo(code, result);
        final CustomEventInit event = CustomEventInit.create();
        event.setDetail(commandInfo);
        fireEvent(IdeSession.EVENT_COMMANDSTARTED, event);

        return result;
    }

    private VariableDefinition[] copyVariables(JsArray<io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.VariableDefinition> list) {
        VariableDefinition[] array = new VariableDefinition[0];
        //noinspection ConstantConditions
        list.forEach((item, p1, p2) -> array[array.length] = new VariableDefinition(item.getName(), item.getType()));
        return array;
    }

    public JsRunnable onLogMessage(JsConsumer<LogItem> callback) {
        return connection.subscribeToLogs(callback);
    }

    public void openDocument(Object params) {
//        connection.getServer().openDocument(result.getHandle(), new DidOpenTextDocumentParams((JsPropertyMap<Object>)params));
        throw new UnsupportedOperationException("openDocument");
    }

    public void changeDocument(Object params) {
//        connection.getServer().changeDocument(result.getHandle(), new DidChangeTextDocumentParams((JsPropertyMap<Object>)params));
        throw new UnsupportedOperationException("changeDocument");
    }

    public Promise<JsArray<CompletionItem>> getCompletionItems(Object params) {
        LazyPromise<CompletionItem[]> promise = new LazyPromise<>();

        throw new UnsupportedOperationException("getCompletionItems");
//        connection.getServer().getCompletionItems(result.getHandle(), new CompletionParams((JsPropertyMap<Object>)params), promise.asCallback());
//
//        return promise.asPromise(BatchOperation.MAX_BATCH_TIME)
//                .then(items->Promise.resolve(makeJsArrayFromCompletionItems(items)));
    }

    public void closeDocument(Object params) {
        throw new UnsupportedOperationException("closeDocument");
//        connection.getServer().closeDocument(result.getHandle(), new DidCloseTextDocumentParams((JsPropertyMap<Object>)params));
    }

    private static JsArray<CompletionItem> makeJsArrayFromCompletionItems(CompletionItem[] items) {
        JsLog.debug("Got completion items", items);
        JsArray<CompletionItem> result = new JsArray<>();
        for (CompletionItem item : items) {
            result.push(item);
        }
        return result;
    }

    public Promise<JsTable> emptyTable(double size, @JsOptional JsPropertyMap<String> columns) {
        return connection.emptyTable(size, columns);
    }

    public Promise<JsTable> timeTable(double periodNanos, @JsOptional DateWrapper startTime) {
        return connection.timeTable(periodNanos, startTime);
    }
}
