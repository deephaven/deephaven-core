//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import com.google.gwt.user.client.Timer;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.core.JsSet;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableResponse;
import io.deephaven.proto.backplane.script.grpc.BrowserNextResponse;
import io.deephaven.proto.backplane.script.grpc.ChangeDocumentRequest;
import io.deephaven.proto.backplane.script.grpc.CloseDocumentRequest;
import io.deephaven.proto.backplane.script.grpc.CompletionContext;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandResponse;
import io.deephaven.proto.backplane.script.grpc.GetCompletionItemsRequest;
import io.deephaven.proto.backplane.script.grpc.GetHoverRequest;
import io.deephaven.proto.backplane.script.grpc.GetSignatureHelpRequest;
import io.deephaven.proto.backplane.script.grpc.OpenDocumentRequest;
import io.deephaven.proto.backplane.script.grpc.Position;
import io.deephaven.proto.backplane.script.grpc.TextDocumentItem;
import io.deephaven.proto.backplane.script.grpc.VersionedTextDocumentIdentifier;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.JsPartitionedTable;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.LogItem;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
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
import io.deephaven.web.shared.ide.lsp.CompletionItem;
import io.deephaven.web.shared.ide.lsp.SignatureInformation;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsNullable;
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
        streamFactory = () -> factory.<BrowserNextResponse>create(
                o -> connection.consoleServiceClient().autoCompleteStream(o),
                (first, o) -> connection.consoleServiceClient().openAutoCompleteStream(first, o),
                (next, c) -> connection.consoleServiceClient().nextAutoCompleteStream(next, c));
    }

    /**
     * Load the named table, with columns and size information already fully populated.
     *
     * @param name the name of the table to fetch
     * @param applyPreviewColumns false to disable previews, defaults to true
     * @return a {@link Promise} that will resolve to the table, or reject with an error if it cannot be loaded.
     * @deprecated Added to resolve a specific issue, in the future preview will be applied as part of the subscription.
     */
    @Deprecated
    public Promise<JsTable> getTable(String name, @JsOptional @JsNullable Boolean applyPreviewColumns) {
        if (applyPreviewColumns == Boolean.FALSE) {
            JsLog.warn(
                    "getTable is deprecated, please use getObject instead. The applyPreviewColumns parameter no longer applies, the new APIs to access data from the resulting Table should be used instead.");
        }
        return connection.getVariableDefinition(name, JsVariableType.TABLE).then(varDef -> {
            final Promise<JsTable> table = connection.getTable(varDef, applyPreviewColumns);
            fireEvent(EVENT_TABLE_OPENED, table);
            return table;
        });
    }

    /**
     * Load the named {@code Figure}, including its tables and tablemaps as needed.
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
        return connection.newTable(columnNames, types, data, userTimeZone).then(table -> {
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
        BindTableToVariableRequest bindRequest = BindTableToVariableRequest.newBuilder()
                .setTableId(table.getHandle().makeTicket())
                .setVariableName(name)
                .build();
        return Callbacks.<BindTableToVariableResponse>grpcUnaryPromise(
                c -> connection.consoleServiceClient().bindTableToVariable(bindRequest, c))
                .then(ignore -> Promise.resolve((Void) null));
    }

    /**
     * Makes the {@code object} available to another user or another client on this same server which knows the value of
     * the {@code sharedTicketBytes}. Use that {@code sharedTicketBytes} value like a one-time use password - any other
     * client which knows this value can read the same object.
     * <p>
     * Shared objects will remain available using the {@code sharedTicketBytes} until the client that first shared them
     * releases/closes their copy of the object. Whatever side-channel is used to share the bytes, be sure to wait until
     * the remote end has signaled that it has successfully fetched the object before releasing it from this client.
     * <p>
     * Be sure to use an unpredictable value for the shared ticket bytes, like a UUID or other large, random value to
     * prevent access by unauthorized clients.
     *
     * @param object The object to share with another client/user.
     * @param sharedTicketBytes The value which another client/user must know to obtain the object. It may be a unicode
     *        string (will be encoded as utf8 bytes), or a {@link elemental2.core.Uint8Array} value.
     * @return A promise that will resolve to the value passed as {@code sharedTicketBytes} when the object is ready to
     *         be read by another client, or will reject if an error occurs.
     */
    public Promise<SharedExportBytesUnion> shareObject(ServerObject.Union object,
            SharedExportBytesUnion sharedTicketBytes) {
        return connection.shareObject(object.asServerObject(), sharedTicketBytes);
    }

    /**
     * Reads an object shared by another client to this server with the {@code sharedTicketBytes}. Until the other
     * client releases this object (or their session ends), the object will be available on the server.
     * <p>
     * The type of the object must be passed so that the object can be read from the server correctly - the other client
     * should provide this information.
     *
     * @param sharedTicketBytes The value provided by another client/user to obtain the object. It may be a unicode
     *        string (will be encoded as utf8 bytes), or a {@link elemental2.core.Uint8Array} value.
     * @param type The type of the object, so it can be correctly read from the server.
     * @return A promise that will resolve to the shared object, or will reject with an error if it cannot be read.
     */
    public Promise<?> getSharedObject(SharedExportBytesUnion sharedTicketBytes, String type) {
        return connection.getSharedObject(sharedTicketBytes, type);
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
        ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
                .setConsoleId(this.result)
                .setCode(code)
                .build();
        Promise<ExecuteCommandResponse> runCodePromise = Callbacks.grpcUnaryPromise(c -> {
            connection.consoleServiceClient().executeCommand(request, c);
        });
        runCodePromise.then(response -> {
            JsVariableChanges changes = JsVariableChanges.from(response.getChanges());
            final long startTimestamp = response.getStartTimestamp();
            final long endTimestamp = response.getEndTimestamp();
            if (response.getErrorMessage().isEmpty()) {
                promise.succeed(new JsCommandResult(changes, null, startTimestamp, endTimestamp));
            } else {
                promise.succeed(new JsCommandResult(changes, response.getErrorMessage(), startTimestamp, endTimestamp));
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
                fireEvent(EVENT_REQUEST_FAILED, status.getDescription());
                pendingAutocompleteCalls.values().forEach(p -> {
                    p.fail("Connection error " + status.getDescription());
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
        final JsPropertyMap<Object> textDoc = jsMap.getAsAny("textDocument").asPropertyMap();
        final OpenDocumentRequest request = OpenDocumentRequest.newBuilder()
                .setConsoleId(result)
                .setTextDocument(TextDocumentItem.newBuilder()
                        .setText(textDoc.getAsAny("text").asString())
                        .setLanguageId(textDoc.getAsAny("languageId").asString())
                        .setUri(textDoc.getAsAny("uri").asString())
                        .setVersion(textDoc.getAsAny("version").asInt()))
                .build();

        JsLog.debug("Opening document for autocomplete ", request);
        AutoCompleteRequest wrapper = AutoCompleteRequest.newBuilder()
                .setConsoleId(result)
                .setOpenDocument(request)
                .build();
        ensureStream().send(wrapper);
    }

    public void changeDocument(Object params) {
        // translate arbitrary value from js to our "properly typed request object".
        final JsPropertyMap<Object> jsMap = (JsPropertyMap<Object>) params;
        final JsPropertyMap<Object> textDoc = jsMap.getAsAny("textDocument").asPropertyMap();
        final ChangeDocumentRequest.Builder request = ChangeDocumentRequest.newBuilder();
        request.setConsoleId(result);
        final VersionedTextDocumentIdentifier textDocument = VersionedTextDocumentIdentifier.newBuilder()
                .setUri(textDoc.getAsAny("uri").asString())
                .setVersion(textDoc.getAsAny("version").asInt())
                .build();
        request.setTextDocument(textDocument);

        final JsArrayLike<Object> changes = jsMap.getAsAny("contentChanges").asArrayLike();
        for (int i = 0; i < changes.getLength(); i++) {
            final JsPropertyMap<Object> change = changes.getAtAsAny(i).asPropertyMap();
            final ChangeDocumentRequest.TextDocumentContentChangeEvent.Builder changeItem =
                    ChangeDocumentRequest.TextDocumentContentChangeEvent.newBuilder();
            changeItem.setText(change.getAsAny("text").asString());
            if (change.has("rangeLength")) {
                changeItem.setRangeLength(change.getAsAny("rangeLength").asInt());
            }
            if (change.has("range")) {
                changeItem.setRange(toRange(change.getAsAny("range")));
            }
            request.addContentChanges(changeItem);
        }

        JsLog.debug("Sending content changes", request);
        AutoCompleteRequest wrapper = AutoCompleteRequest.newBuilder()
                .setConsoleId(result)
                .setChangeDocument(request)
                .build();
        ensureStream().send(wrapper);
    }

    private DocumentRange toRange(final Any range) {
        final JsPropertyMap<Object> rangeObj = range.asPropertyMap();
        return DocumentRange.newBuilder()
                .setStart(toPosition(rangeObj.getAsAny("start")))
                .setEnd(toPosition(rangeObj.getAsAny("end")))
                .build();
    }

    private Position toPosition(final Any pos) {
        final JsPropertyMap<Object> posObj = pos.asPropertyMap();
        return Position.newBuilder()
                .setLine(posObj.getAsAny("line").asInt())
                .setCharacter(posObj.getAsAny("character").asInt())
                .build();
    }

    private AutoCompleteRequest.Builder getAutoCompleteRequest() {
        return AutoCompleteRequest.newBuilder()
                .setConsoleId(this.result)
                .setRequestId(nextAutocompleteRequestId++);
    }

    public Promise<JsArray<CompletionItem>> getCompletionItems(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        final GetCompletionItemsRequest completionRequest = GetCompletionItemsRequest.newBuilder()

                .setTextDocument(textDocument)
                .setPosition(toPosition(jsMap.getAsAny("position")))
                .setContext(toContext(jsMap.getAsAny("context")))
                .build();

        LazyPromise<AutoCompleteResponse> promise = new LazyPromise<>();

        final AutoCompleteRequest request = getAutoCompleteRequest()
                .setGetCompletionItems(completionRequest)
                .build();
        pendingAutocompleteCalls.put(request.getRequestId(), promise);
        ensureStream().send(request);

        return promise
                .timeout(JsTable.MAX_BATCH_TIME)
                .asPromise()
                .then(res -> {
                    JsArray<CompletionItem> results = Js.uncheckedCast(res.getCompletionItems().getItemsList()
                            .stream()
                            .map(LspTranslate::toJs)
                            .toArray());
                    return Promise.resolve(results);
                },
                        fail -> {
                            // noinspection unchecked, rawtypes
                            return (Promise<JsArray<CompletionItem>>) (Promise) Promise
                                    .reject(fail);
                        });
    }


    public Promise<JsArray<io.deephaven.web.shared.ide.lsp.SignatureInformation>> getSignatureHelp(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        final GetSignatureHelpRequest signatureHelpRequest = GetSignatureHelpRequest.newBuilder()
                .setTextDocument(textDocument)
                .setPosition(toPosition(jsMap.getAsAny("position")))
                .build();

        final AutoCompleteRequest request = getAutoCompleteRequest().setGetSignatureHelp(signatureHelpRequest)
                .build();

        LazyPromise<AutoCompleteResponse> promise = new LazyPromise<>();
        pendingAutocompleteCalls.put(request.getRequestId(), promise);
        ensureStream().send(request);

        return promise
                .timeout(JsTable.MAX_BATCH_TIME)
                .asPromise()
                .then(res -> {
                    JsArray<SignatureInformation> array = Js.uncheckedCast(res.getSignatures().getSignaturesList()
                            .stream()
                            .map(LspTranslate::toJs)
                            .toArray());
                    return Promise.resolve(array);
                },
                        fail -> {
                            // noinspection unchecked, rawtypes
                            return (Promise<JsArray<io.deephaven.web.shared.ide.lsp.SignatureInformation>>) (Promise) Promise
                                    .reject(fail);
                        });
    }

    public Promise<io.deephaven.web.shared.ide.lsp.Hover> getHover(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final VersionedTextDocumentIdentifier textDocument = toVersionedTextDoc(jsMap.getAsAny("textDocument"));
        final GetHoverRequest hoverRequest = GetHoverRequest.newBuilder()
                .setTextDocument(textDocument)
                .setPosition(toPosition(jsMap.getAsAny("position")))
                .build();

        final AutoCompleteRequest request = getAutoCompleteRequest().setGetHover(hoverRequest).build();

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
        final CompletionContext.Builder result = CompletionContext.newBuilder();
        if (contextObj.has("triggerCharacter")) {
            result.setTriggerCharacter(contextObj.getAsAny("triggerCharacter").asString());
        }
        result.setTriggerKind(contextObj.getAsAny("triggerKind").asInt());
        return result.build();
    }

    public void closeDocument(Object params) {
        final JsPropertyMap<Object> jsMap = Js.uncheckedCast(params);
        final CloseDocumentRequest request = CloseDocumentRequest.newBuilder()
                .setConsoleId(result)
                .setTextDocument(toVersionedTextDoc(jsMap.getAsAny("textDocument")))
                .build();

        JsLog.debug("Closing document for autocomplete ", request);
        AutoCompleteRequest wrapper = AutoCompleteRequest.newBuilder()
                .setConsoleId(result)
                .setCloseDocument(request)
                .build();
        ensureStream().send(wrapper);
    }

    private VersionedTextDocumentIdentifier toVersionedTextDoc(final Any textDoc) {
        final JsPropertyMap<Object> textDocObj = textDoc.asPropertyMap();
        final VersionedTextDocumentIdentifier.Builder textDocument = VersionedTextDocumentIdentifier.newBuilder();
        textDocument.setUri(textDocObj.getAsAny("uri").asString());
        if (textDocObj.has("version")) {
            textDocument.setVersion(textDocObj.getAsAny("version").asInt());
        }
        return textDocument.build();
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
    public Promise<JsTable> timeTable(double periodNanos, @JsOptional @JsNullable DateWrapper startTime) {
        return connection.timeTable(periodNanos, startTime);
    }
}
