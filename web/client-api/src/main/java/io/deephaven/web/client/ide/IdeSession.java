/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.ide;

import com.google.gwt.user.client.Timer;
import elemental2.core.JsArray;
import elemental2.core.JsSet;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.changedocumentrequest.TextDocumentContentChangeEvent;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.console.JsCommandResult;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.widget.plot.JsFigure;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.LogItem;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.ide.ExecutionHandle;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;
import jsinterop.base.JsPropertyMap;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static io.deephaven.web.client.api.QueryConnectable.EVENT_TABLE_OPENED;

/**
 */
@JsType(namespace = "dh")
public class IdeSession extends HasEventHandling {
    private static final int AUTOCOMPLETE_STREAM_TIMEOUT = 30_000;

    public static final String EVENT_COMMANDSTARTED = "commandstarted";

    private final Ticket result;

    private final JsSet<ExecutionHandle> cancelled;
    private final WorkerConnection connection;
    private final JsRunnable closer;
    private int nextAutocompleteRequestId = 0;
    private Map<Integer, LazyPromise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>>> pendingAutocompleteCalls =
            new HashMap<>();

    private final Supplier<BiDiStream<AutoCompleteRequest, AutoCompleteResponse>> streamFactory;
    private BiDiStream<AutoCompleteRequest, AutoCompleteResponse> currentStream;
    private final Timer autocompleteStreamCloseTimeout = new Timer() {
        @Override
        public void run() {
            assert currentStream != null;
            if (!pendingAutocompleteCalls.isEmpty()) {
                // apparently waiting on something, keep waiting
                autocompleteStreamCloseTimeout.schedule(AUTOCOMPLETE_STREAM_TIMEOUT);
                return;
            }
            currentStream.end();
            currentStream.cancel();
            currentStream = null;
        }
    };

    @JsIgnore
    public IdeSession(
            WorkerConnection connection,
            Ticket connectionResult,
            JsRunnable closer) {
        this.result = connectionResult;
        cancelled = new JsSet<>();
        this.connection = connection;
        this.closer = closer;

        BiDiStream.Factory<AutoCompleteRequest, AutoCompleteResponse> factory = connection.streamFactory();
        streamFactory = () -> factory.create(
                connection.consoleServiceClient()::autoCompleteStream,
                (first, headers) -> connection.consoleServiceClient().openAutoCompleteStream(first, headers),
                (next, headers, c) -> connection.consoleServiceClient().nextAutoCompleteStream(next, headers, c::apply),
                new AutoCompleteRequest());
    }

    // TODO (deephaven-core#188): improve usage of subscriptions (w.r.t. this optional param)
    public Promise<JsTable> getTable(String name, @JsOptional Boolean applyPreviewColumns) {
        return connection.getVariableDefinition(name, JsVariableChanges.TABLE).then(varDef -> {
            final Promise<JsTable> table = connection.getTable(varDef, applyPreviewColumns);
            final CustomEventInit event = CustomEventInit.create();
            event.setDetail(table);
            fireEvent(EVENT_TABLE_OPENED, event);
            return table;
        });
    }

    public Promise<JsFigure> getFigure(String name) {
        return connection.getVariableDefinition(name, JsVariableChanges.FIGURE).then(connection::getFigure);
    }

    public Promise<JsTreeTable> getTreeTable(String name) {
        return connection.getVariableDefinition(name, JsVariableChanges.HIERARCHICALTABLE)
                .then(connection::getTreeTable);
    }

    public Promise<JsTreeTable> getHierarchicalTable(String name) {
        return connection.getVariableDefinition(name, JsVariableChanges.HIERARCHICALTABLE)
                .then(connection::getTreeTable);
    }

    public Promise<?> getObject(JsPropertyMap<Object> definitionObject) {
        return connection.getJsObject(definitionObject);
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
        bindRequest.setTableId(table.getHandle().makeTicket());
        bindRequest.setVariableName(name);
        return Callbacks
                .grpcUnaryPromise(c -> connection.consoleServiceClient().bindTableToVariable(bindRequest,
                        connection.metadata(), c::apply))
                .then(ignore -> Promise.resolve((Void) null));
    }

    public JsRunnable subscribeToFieldUpdates(JsConsumer<JsVariableChanges> callback) {
        return connection.subscribeToFieldUpdates(callback);
    }

    public void close() {
        pendingAutocompleteCalls.clear();// let the timer clean up the rest for now
        closer.run();
    }

    public CancellablePromise<JsCommandResult> runCode(String code) {
        LazyPromise<JsCommandResult> promise = new LazyPromise<>();
        ExecuteCommandRequest request = new ExecuteCommandRequest();
        request.setConsoleId(this.result);
        request.setCode(code);
        Promise<ExecuteCommandResponse> runCodePromise = Callbacks.grpcUnaryPromise(c -> {
            connection.consoleServiceClient().executeCommand(request, connection.metadata(), c::apply);
        });
        runCodePromise.then(response -> {
            JsVariableChanges changes = JsVariableChanges.from(response.getChanges());
            promise.succeed(new JsCommandResult(changes, response.getErrorMessage()));
            return null;
        }, err -> {
            promise.fail(err);
            return null;
        });

        CancellablePromise<JsCommandResult> result = promise.asPromise(
                () -> {
                    // cancelled.add(handle);
                    // CancelCommandRequest cancelRequest = new CancelCommandRequest();
                    // cancelRequest.setCommandid();
                    // connection.consoleServiceClient().cancelCommand(cancelRequest, connection.metadata());
                    throw new UnsupportedOperationException("cancelCommand");
                });

        CommandInfo commandInfo = new CommandInfo(code, result);
        final CustomEventInit event = CustomEventInit.create();
        event.setDetail(commandInfo);
        fireEvent(IdeSession.EVENT_COMMANDSTARTED, event);

        return result;
    }

    public JsRunnable onLogMessage(JsConsumer<LogItem> callback) {
        return connection.subscribeToLogs(callback);
    }


    private BiDiStream<AutoCompleteRequest, AutoCompleteResponse> ensureStream() {
        autocompleteStreamCloseTimeout.schedule(AUTOCOMPLETE_STREAM_TIMEOUT);
        if (currentStream != null) {
            return currentStream;
        }
        currentStream = streamFactory.get();
        currentStream.onData(res -> {
            LazyPromise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>> pendingPromise =
                    pendingAutocompleteCalls.remove(res.getCompletionItems().getRequestId());
            if (pendingPromise == null) {
                return;
            }
            if (res.getCompletionItems().getSuccess()) {
                pendingPromise.succeed(cleanupItems(res.getCompletionItems().getItemsList()));
            } else {
                pendingPromise
                        .fail("Error occurred handling autocomplete on the server, probably request is out of date");
            }
        });
        currentStream.onStatus(status -> {
            if (!status.isOk()) {
                CustomEventInit init = CustomEventInit.create();
                init.setDetail(status.getDetails());
                fireEvent(EVENT_REQUEST_FAILED, init);
                pendingAutocompleteCalls.values().forEach(p -> {
                    p.fail("Connection error" + status.getDetails());
                });
                pendingAutocompleteCalls.clear();
            }
        });
        currentStream.onEnd(status -> {
            currentStream = null;
            autocompleteStreamCloseTimeout.cancel();
            pendingAutocompleteCalls.clear();
        });
        return currentStream;
    }

    public void openDocument(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final OpenDocumentRequest request = new OpenDocumentRequest();

        request.setConsoleId(result);
        final JsPropertyMap<Object> textDoc = jsMap.getAsAny("textDocument").asPropertyMap();
        final TextDocumentItem textDocument = new TextDocumentItem();
        textDocument.setText(textDoc.getAsAny("text").asString());
        textDocument.setLanguageId(textDoc.getAsAny("languageId").asString());
        textDocument.setUri(textDoc.getAsAny("uri").asString());
        textDocument.setVersion(textDoc.getAsAny("version").asDouble());
        request.setTextDocument(textDocument);

        JsLog.debug("Opening document for autocomplete ", request);
        AutoCompleteRequest wrapper = new AutoCompleteRequest();
        wrapper.setOpenDocument(request);
        ensureStream().send(wrapper);
    }

    public void changeDocument(Object params) {
        // translate arbitrary value from js to our "properly typed request object".
        final JsPropertyMap<Object> jsMap = (JsPropertyMap<Object>) params;
        final ChangeDocumentRequest request = new ChangeDocumentRequest();
        request.setConsoleId(result);
        final JsPropertyMap<Object> textDoc = jsMap.getAsAny("textDocument").asPropertyMap();
        final VersionedTextDocumentIdentifier textDocument = new VersionedTextDocumentIdentifier();
        textDocument.setUri(textDoc.getAsAny("uri").asString());
        textDocument.setVersion(textDoc.getAsAny("version").asDouble());
        request.setTextDocument(textDocument);

        final JsArrayLike<Object> changes = jsMap.getAsAny("contentChanges").asArrayLike();
        final JsArray<TextDocumentContentChangeEvent> changeList = new JsArray<>();
        for (int i = 0; i < changes.getLength(); i++) {
            final JsPropertyMap<Object> change = changes.getAtAsAny(i).asPropertyMap();
            final TextDocumentContentChangeEvent changeItem = new TextDocumentContentChangeEvent();
            changeItem.setText(change.getAsAny("text").asString());
            if (change.has("rangeLength")) {
                changeItem.setRangeLength(change.getAsAny("rangeLength").asInt());
            }
            if (change.has("range")) {
                changeItem.setRange(toRange(change.getAsAny("range")));
            }
            changeList.push(changeItem);
        }
        request.setContentChangesList(changeList);

        JsLog.debug("Sending content changes", request);
        AutoCompleteRequest wrapper = new AutoCompleteRequest();
        wrapper.setChangeDocument(request);
        ensureStream().send(wrapper);
    }

    private DocumentRange toRange(final Any range) {
        final JsPropertyMap<Object> rangeObj = range.asPropertyMap();
        final DocumentRange result = new DocumentRange();
        result.setStart(toPosition(rangeObj.getAsAny("start")));
        result.setEnd(toPosition(rangeObj.getAsAny("end")));
        return result;
    }

    private Position toPosition(final Any pos) {
        final JsPropertyMap<Object> posObj = pos.asPropertyMap();
        final Position result = new Position();
        result.setLine(posObj.getAsAny("line").asInt());
        result.setCharacter(posObj.getAsAny("character").asInt());
        return result;
    }

    public Promise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>> getCompletionItems(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final GetCompletionItemsRequest request = new GetCompletionItemsRequest();

        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        request.setTextDocument(textDocument);
        request.setPosition(toPosition(jsMap.getAsAny("position")));
        request.setContext(toContext(jsMap.getAsAny("context")));
        request.setConsoleId(this.result);
        request.setRequestId(nextAutocompleteRequestId++);

        LazyPromise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>> promise = new LazyPromise<>();
        AutoCompleteRequest wrapper = new AutoCompleteRequest();
        wrapper.setGetCompletionItems(request);
        ensureStream().send(wrapper);
        pendingAutocompleteCalls.put(request.getRequestId(), promise);

        return promise
                .timeout(JsTable.MAX_BATCH_TIME)
                .asPromise()
                .then(Promise::resolve, fail -> {
                    pendingAutocompleteCalls.remove(request.getRequestId());
                    // noinspection unchecked, rawtypes
                    return (Promise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>>) (Promise) Promise
                            .reject(fail);
                });
    }

    private JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem> cleanupItems(
            final JsArray<CompletionItem> itemsList) {
        JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem> cleaned = new JsArray<>();
        if (itemsList != null) {
            for (int i = 0; i < itemsList.getLength(); i++) {
                final CompletionItem item = itemsList.getAt(i);
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
            result.setTriggerCharacter(contextObj.getAsAny("triggerCharacter").asString());
        }
        result.setTriggerKind(contextObj.getAsAny("triggerKind").asInt());
        return result;
    }

    public void closeDocument(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final CloseDocumentRequest request = new CloseDocumentRequest();
        request.setConsoleId(result);
        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        request.setTextDocument(textDocument);

        JsLog.debug("Closing document for autocomplete ", request);
        AutoCompleteRequest wrapper = new AutoCompleteRequest();
        wrapper.setCloseDocument(request);
        ensureStream().send(wrapper);
    }

    private VersionedTextDocumentIdentifier toVersionedTextDoc(final Any textDoc) {
        final JsPropertyMap<Object> textDocObj = textDoc.asPropertyMap();
        final VersionedTextDocumentIdentifier textDocument = new VersionedTextDocumentIdentifier();
        textDocument.setUri(textDocObj.getAsAny("uri").asString());
        if (textDocObj.has("version")) {
            textDocument.setVersion(textDocObj.getAsAny("version").asDouble());
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
