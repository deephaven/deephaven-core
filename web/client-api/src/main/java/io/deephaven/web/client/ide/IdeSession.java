//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import com.google.gwt.user.client.Timer;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.core.JsSet;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.AutoCompleteRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.AutoCompleteResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.BindTableToVariableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.ChangeDocumentRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.CloseDocumentRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.CompletionContext;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.DocumentRange;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.ExecuteCommandRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.ExecuteCommandResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.GetCompletionItemsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.GetHoverRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.GetSignatureHelpRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.OpenDocumentRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.Position;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.TextDocumentItem;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.VersionedTextDocumentIdentifier;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.changedocumentrequest.TextDocumentContentChangeEvent;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.console.JsCommandResult;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDescriptor;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.widget.plot.JsFigure;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
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

import static io.deephaven.web.client.api.QueryInfoConstants.EVENT_TABLE_OPENED;

/**
 */
@JsType(namespace = "dh")
public class IdeSession extends HasEventHandling {
    private static final int AUTOCOMPLETE_STREAM_TIMEOUT = 30_000;

    public static final String EVENT_COMMANDSTARTED = "commandstarted",
            EVENT_REQUEST_FAILED = "requestfailed";

    private final Ticket result;

    private final JsSet<ExecutionHandle> cancelled;
    private final WorkerConnection connection;
    private final JsRunnable closer;
    private int nextAutocompleteRequestId = 0;
    private Map<Integer, LazyPromise<AutoCompleteResponse>> pendingAutocompleteCalls =
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

    /**
     * Load the named table, with columns and size information already fully populated.
     * 
     * @param name
     * @param applyPreviewColumns optional boolean
     * @return {@link Promise} of {@link JsTable}
     */
    // TODO (deephaven-core#188): improve usage of subscriptions (w.r.t. this optional param)
    public Promise<JsTable> getTable(String name, @JsOptional Boolean applyPreviewColumns) {
        return connection.getVariableDefinition(name, JsVariableType.TABLE).then(varDef -> {
            final Promise<JsTable> table = connection.getTable(varDef, applyPreviewColumns);
            fireEvent(EVENT_TABLE_OPENED, table);
            return table;
        });
    }

    /**
     * Load the named Figure, including its tables and tablemaps as needed.
     * 
     * @param name
     * @return promise of dh.plot.Figure
     */
    public Promise<JsFigure> getFigure(String name) {
        return connection.getVariableDefinition(name, JsVariableType.FIGURE).then(connection::getFigure);
    }

    /**
     * Loads the named tree table or roll-up table, with column data populated. All nodes are collapsed by default, and
     * size is presently not available until the viewport is first set.
     * 
     * @param name
     * @return {@link Promise} of {@link JsTreeTable}
     */
    public Promise<JsTreeTable> getTreeTable(String name) {
        return connection.getVariableDefinition(name, JsVariableType.HIERARCHICALTABLE)
                .then(connection::getHierarchicalTable);
    }

    public Promise<JsTreeTable> getHierarchicalTable(String name) {
        return connection.getVariableDefinition(name, JsVariableType.HIERARCHICALTABLE)
                .then(connection::getHierarchicalTable);
    }

    public Promise<JsPartitionedTable> getPartitionedTable(String name) {
        return connection.getVariableDefinition(name, JsVariableType.PARTITIONEDTABLE)
                .then(connection::getPartitionedTable);
    }

    public Promise<?> getObject(@TsTypeRef(JsVariableDescriptor.class) JsPropertyMap<Object> definitionObject) {
        return connection.getJsObject(definitionObject);
    }

    public Promise<JsTable> newTable(String[] columnNames, String[] types, String[][] data, String userTimeZone) {
        return connection.newTable(columnNames, types, data, userTimeZone, this).then(table -> {
            fireEvent(EVENT_TABLE_OPENED, table);

            return Promise.resolve(table);
        });
    }

    /**
     * Merges the given tables into a single table. Assumes all tables have the same structure.
     * 
     * @param tables
     * @return {@link Promise} of {@link JsTable}
     */
    public Promise<JsTable> mergeTables(JsTable[] tables) {
        return connection.mergeTables(tables, this).then(table -> {
            fireEvent(EVENT_TABLE_OPENED, table);

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
            if (response.getErrorMessage() == null || response.getErrorMessage().isEmpty()) {
                promise.succeed(new JsCommandResult(changes, null));
            } else {
                promise.succeed(new JsCommandResult(changes, response.getErrorMessage()));
            }
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
        fireEvent(IdeSession.EVENT_COMMANDSTARTED, commandInfo);

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
            if (res.getSuccess()) {
                pendingAutocompleteCalls.remove(res.getRequestId()).succeed(res);
            } else {
                pendingAutocompleteCalls.remove(res.getRequestId())
                        .fail("Error occurred handling autocomplete on the server, probably request is out of date");
            }
        });
        currentStream.onStatus(status -> {
            if (!status.isOk()) {
                fireEvent(EVENT_REQUEST_FAILED, status.getDetails());
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
        wrapper.setConsoleId(result);
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
        wrapper.setConsoleId(result);
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

    private AutoCompleteRequest getAutoCompleteRequest() {
        AutoCompleteRequest request = new AutoCompleteRequest();
        request.setConsoleId(this.result);
        request.setRequestId(nextAutocompleteRequestId);
        nextAutocompleteRequestId++;
        return request;
    }

    public Promise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>> getCompletionItems(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final GetCompletionItemsRequest completionRequest = new GetCompletionItemsRequest();

        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        completionRequest.setTextDocument(textDocument);
        completionRequest.setPosition(toPosition(jsMap.getAsAny("position")));
        completionRequest.setContext(toContext(jsMap.getAsAny("context")));

        final AutoCompleteRequest request = getAutoCompleteRequest();
        request.setGetCompletionItems(completionRequest);

        // Set these in case running against an old server implementation
        completionRequest.setConsoleId(request.getConsoleId());
        completionRequest.setRequestId(request.getRequestId());

        LazyPromise<AutoCompleteResponse> promise = new LazyPromise<>();
        pendingAutocompleteCalls.put(request.getRequestId(), promise);
        ensureStream().send(request);

        return promise
                .timeout(JsTable.MAX_BATCH_TIME)
                .asPromise()
                .then(res -> Promise.resolve(
                        res.getCompletionItems().getItemsList().map((item, index) -> LspTranslate.toJs(item))),
                        fail -> {
                            // noinspection unchecked, rawtypes
                            return (Promise<JsArray<io.deephaven.web.shared.ide.lsp.CompletionItem>>) (Promise) Promise
                                    .reject(fail);
                        });
    }


    public Promise<JsArray<io.deephaven.web.shared.ide.lsp.SignatureInformation>> getSignatureHelp(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final GetSignatureHelpRequest signatureHelpRequest = new GetSignatureHelpRequest();

        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        signatureHelpRequest.setTextDocument(textDocument);
        signatureHelpRequest.setPosition(toPosition(jsMap.getAsAny("position")));

        final AutoCompleteRequest request = getAutoCompleteRequest();
        request.setGetSignatureHelp(signatureHelpRequest);

        LazyPromise<AutoCompleteResponse> promise = new LazyPromise<>();
        pendingAutocompleteCalls.put(request.getRequestId(), promise);
        ensureStream().send(request);

        return promise
                .timeout(JsTable.MAX_BATCH_TIME)
                .asPromise()
                .then(res -> Promise.resolve(
                        res.getSignatures().getSignaturesList().map((item, index) -> LspTranslate.toJs(item))),
                        fail -> {
                            // noinspection unchecked, rawtypes
                            return (Promise<JsArray<io.deephaven.web.shared.ide.lsp.SignatureInformation>>) (Promise) Promise
                                    .reject(fail);
                        });
    }

    public Promise<io.deephaven.web.shared.ide.lsp.Hover> getHover(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final GetHoverRequest hoverRequest = new GetHoverRequest();

        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        hoverRequest.setTextDocument(textDocument);
        hoverRequest.setPosition(toPosition(jsMap.getAsAny("position")));

        final AutoCompleteRequest request = getAutoCompleteRequest();
        request.setGetHover(hoverRequest);

        LazyPromise<AutoCompleteResponse> promise = new LazyPromise<>();
        pendingAutocompleteCalls.put(request.getRequestId(), promise);
        ensureStream().send(request);

        return promise
                .timeout(JsTable.MAX_BATCH_TIME)
                .asPromise()
                .then(res -> Promise.resolve(LspTranslate.toJs(res.getHover())),
                        fail -> {
                            // noinspection unchecked, rawtypes
                            return (Promise<io.deephaven.web.shared.ide.lsp.Hover>) (Promise) Promise
                                    .reject(fail);
                        });
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
        wrapper.setConsoleId(result);
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

    /**
     * Creates an empty table with the specified number of rows. Optionally columns and types may be specified, but all
     * values will be null.
     * 
     * @param size
     * @return {@link Promise} of {@link JsTable}
     */
    public Promise<JsTable> emptyTable(double size) {
        return connection.emptyTable(size);
    }

    /**
     * Creates a new table that ticks automatically every "periodNanos" nanoseconds. A start time may be provided; if so
     * the table will be populated with the interval from the specified date until now.
     * 
     * @param periodNanos
     * @param startTime
     * @return {@link Promise} of {@link JsTable}
     */
    public Promise<JsTable> timeTable(double periodNanos, @JsOptional DateWrapper startTime) {
        return connection.timeTable(periodNanos, startTime);
    }
}
