package io.deephaven.ide.shared;

import elemental2.core.JsArray;
import elemental2.core.JsSet;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.changedocumentrequest.TextDocumentContentChangeEvent;
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
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;
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
        JsRunnable closer) {
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

    // TODO: #37: Need SmartKey support for this functionality
    @JsIgnore
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

    public Promise<JsTable> newTable(String[] columnNames, String[] types, String[][] data,
        String userTimeZone) {
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
        bindRequest.setTableId(table.getHandle().makeTicket());
        bindRequest.setVariableName(name);
        return Callbacks
            .grpcUnaryPromise(c -> connection.consoleServiceClient()
                .bindTableToVariable(bindRequest, connection.metadata(), c::apply))
            .then(ignore -> Promise.resolve((Void) null));
    }

    public void close() {
        closer.run();
    }

    public CancellablePromise<JsCommandResult> runCode(String code) {
        LazyPromise<CommandResult> promise = new LazyPromise<>();
        ExecuteCommandRequest request = new ExecuteCommandRequest();
        request.setConsoleId(this.result);
        request.setCode(code);
        Promise<ExecuteCommandResponse> runCodePromise = Callbacks.grpcUnaryPromise(c -> {
            connection.consoleServiceClient().executeCommand(request, connection.metadata(),
                c::apply);
        });
        runCodePromise.then(response -> {
            CommandResult commandResult = new CommandResult();
            commandResult.setError(response.getErrorMessage());
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
                // cancelled.add(handle);
                // CancelCommandRequest cancelRequest = new CancelCommandRequest();
                // cancelRequest.setCommandid();
                // connection.consoleServiceClient().cancelCommand(cancelRequest,
                // connection.metadata());
                throw new UnsupportedOperationException("cancelCommand");
            });

        CommandInfo commandInfo = new CommandInfo(code, result);
        final CustomEventInit event = CustomEventInit.create();
        event.setDetail(commandInfo);
        fireEvent(IdeSession.EVENT_COMMANDSTARTED, event);

        return result;
    }

    private VariableDefinition[] copyVariables(
        JsArray<io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.VariableDefinition> list) {
        VariableDefinition[] array = new VariableDefinition[0];
        // noinspection ConstantConditions
        list.forEach((item, p1, p2) -> array[array.length] =
            new VariableDefinition(item.getName(), item.getType()));
        return array;
    }

    public JsRunnable onLogMessage(JsConsumer<LogItem> callback) {
        return connection.subscribeToLogs(callback);
    }

    public void openDocument(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final OpenDocumentRequest request = new OpenDocumentRequest();

        request.setConsoleId(result);
        final JsPropertyMap<Object> textDoc = jsMap.getAny("textDocument").asPropertyMap();
        final TextDocumentItem textDocument = new TextDocumentItem();
        textDocument.setText(textDoc.getAny("text").asString());
        textDocument.setLanguageId(textDoc.getAny("languageId").asString());
        textDocument.setUri(textDoc.getAny("uri").asString());
        textDocument.setVersion(textDoc.getAny("version").asDouble());
        request.setTextDocument(textDocument);

        JsLog.debug("Opening document for autocomplete ", request);
        connection.consoleServiceClient().openDocument(request, connection.metadata(),
            (p0, p1) -> JsLog.debug("open doc response", p0, p1));
    }

    public void changeDocument(Object params) {
        // translate arbitrary value from js to our "properly typed request object".
        final JsPropertyMap<Object> jsMap = (JsPropertyMap<Object>) params;
        final ChangeDocumentRequest request = new ChangeDocumentRequest();
        request.setConsoleId(result);
        final JsPropertyMap<Object> textDoc = jsMap.getAny("textDocument").asPropertyMap();
        final VersionedTextDocumentIdentifier textDocument = new VersionedTextDocumentIdentifier();
        textDocument.setUri(textDoc.getAny("uri").asString());
        textDocument.setVersion(textDoc.getAny("version").asDouble());
        request.setTextDocument(textDocument);

        final JsArrayLike<Object> changes = jsMap.getAny("contentChanges").asArrayLike();
        final JsArray<TextDocumentContentChangeEvent> changeList = new JsArray<>();
        for (int i = 0; i < changes.getLength(); i++) {
            final JsPropertyMap<Object> change = changes.getAnyAt(i).asPropertyMap();
            final TextDocumentContentChangeEvent changeItem = new TextDocumentContentChangeEvent();
            changeItem.setText(change.getAny("text").asString());
            if (change.has("rangeLength")) {
                changeItem.setRangeLength(change.getAny("rangeLength").asInt());
            }
            if (change.has("range")) {
                changeItem.setRange(toRange(change.getAny("range")));
            }
            changeList.push(changeItem);
        }
        request.setContentChangesList(changeList);

        JsLog.debug("Sending content changes", request);
        connection.consoleServiceClient().changeDocument(request, connection.metadata(),
            (p0, p1) -> JsLog.debug("Updated doc", p0, p1));
    }

    private DocumentRange toRange(final Any range) {
        final JsPropertyMap<Object> rangeObj = range.asPropertyMap();
        final DocumentRange result = new DocumentRange();
        result.setStart(toPosition(rangeObj.getAny("start")));
        result.setEnd(toPosition(rangeObj.getAny("end")));
        return result;
    }

    private Position toPosition(final Any pos) {
        final JsPropertyMap<Object> posObj = pos.asPropertyMap();
        final Position result = new Position();
        result.setLine(posObj.getAny("line").asInt());
        result.setCharacter(posObj.getAny("character").asInt());
        return result;
    }

    public Promise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>> getCompletionItems(
        Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final GetCompletionItemsRequest request = new GetCompletionItemsRequest();

        final VersionedTextDocumentIdentifier textDocument =
            toVersionedTextDoc(jsMap.getAny("textDocument"));
        request.setTextDocument(textDocument);
        request.setPosition(toPosition(jsMap.getAny("position")));
        request.setContext(toContext(jsMap.getAny("context")));
        request.setConsoleId(this.result);

        LazyPromise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>> promise =
            new LazyPromise<>();
        connection.consoleServiceClient().getCompletionItems(request, connection.metadata(),
            (p0, p1) -> {
                JsLog.debug("Got completions", p0, p1);
                promise.succeed(cleanupItems(p1.getItemsList()));
            });

        return promise.asPromise(JsTable.MAX_BATCH_TIME)
            .then(Promise::resolve);
    }

    private JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem> cleanupItems(
        final JsArray itemsList) {
        JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem> cleaned = new JsArray<>();
        if (itemsList != null) {
            for (int i = 0; i < itemsList.getLength(); i++) {
                final CompletionItem item = (CompletionItem) itemsList.getAt(i);
                final io.deephaven.web.shared.ide.lsp.CompletionItem copy = LspTranslate.toJs(item);
                cleaned.push(copy);
            }
        }
        return cleaned;
    }

    private CompletionContext toContext(final Any context) {
        JsLog.debug("toContext", context);
        final JsPropertyMap<Object> contextObj = context.asPropertyMap();
        final CompletionContext result = new CompletionContext();
        if (contextObj.has("triggerCharacter")) {
            result.setTriggerCharacter(contextObj.getAny("triggerCharacter").asString());
        }
        result.setTriggerKind(contextObj.getAny("triggerKind").asInt());
        return result;
    }

    public void closeDocument(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final CloseDocumentRequest request = new CloseDocumentRequest();
        request.setConsoleId(result);
        final VersionedTextDocumentIdentifier textDocument =
            toVersionedTextDoc(jsMap.getAny("textDocument"));
        request.setTextDocument(textDocument);

        JsLog.debug("Closing document for autocomplete ", request);
        connection.consoleServiceClient().closeDocument(request, connection.metadata(),
            (p0, p1) -> JsLog.debug("response back", p0, p1));
    }

    private VersionedTextDocumentIdentifier toVersionedTextDoc(final Any textDoc) {
        final JsPropertyMap<Object> textDocObj = textDoc.asPropertyMap();
        final VersionedTextDocumentIdentifier textDocument = new VersionedTextDocumentIdentifier();
        textDocument.setUri(textDocObj.getAny("uri").asString());
        if (textDocObj.has("version")) {
            textDocument.setVersion(textDocObj.getAny("version").asDouble());
        }
        return textDocument;
    }

    public Promise<JsTable> emptyTable(double size) {
        return connection.emptyTable(size);
    }

    public Promise<JsTable> timeTable(double periodNanos, @JsOptional DateWrapper startTime) {
        return connection.timeTable(periodNanos, startTime);
    }
}
