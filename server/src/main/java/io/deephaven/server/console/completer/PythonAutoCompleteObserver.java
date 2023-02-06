package io.deephaven.server.console.completer;

import com.google.rpc.Code;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.proto.backplane.script.grpc.*;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.session.SessionCloseableObserver;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jpy.PyObject;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.List;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyOnNext;

/**
 * Autocomplete handling for python that will use the jedi library, if it is installed.
 */
public class PythonAutoCompleteObserver extends SessionCloseableObserver<AutoCompleteResponse>
        implements StreamObserver<AutoCompleteRequest> {

    private static final Logger log = LoggerFactory.getLogger(PythonAutoCompleteObserver.class);

    /**
     * We only log timing for completions that take longer than, currently, 100ms
     */
    private static final long HUNDRED_MS_IN_NS = 100_000_000;
    private final Provider<ScriptSession> scriptSession;

    public PythonAutoCompleteObserver(StreamObserver<AutoCompleteResponse> responseObserver,
            Provider<ScriptSession> scriptSession, final SessionState session) {
        super(session, responseObserver);
        this.scriptSession = scriptSession;
    }

    @Override
    @SuppressWarnings("DuplicatedCode")
    public void onNext(AutoCompleteRequest value) {
        switch (value.getRequestCase()) {
            case OPEN_DOCUMENT: {
                final OpenDocumentRequest openDoc = value.getOpenDocument();
                final TextDocumentItem doc = openDoc.getTextDocument();
                PyObject completer = (PyObject) scriptSession.get().getVariable("jedi_settings");
                completer.callMethod("open_doc", doc.getText(), doc.getUri(), doc.getVersion());
                break;
            }
            case CHANGE_DOCUMENT: {
                ChangeDocumentRequest request = value.getChangeDocument();
                final VersionedTextDocumentIdentifier text = request.getTextDocument();

                PyObject completer = (PyObject) scriptSession.get().getVariable("jedi_settings");
                String uri = text.getUri();
                int version = text.getVersion();
                String document = completer.callMethod("get_doc", text.getUri()).getStringValue();

                final List<ChangeDocumentRequest.TextDocumentContentChangeEvent> changes =
                        request.getContentChangesList();
                document = CompletionParser.updateDocumentChanges(uri, version, document, changes);
                if (document == null) {
                    return;
                }

                completer.callMethod("update_doc", document, uri, version);
                break;
            }
            case GET_COMPLETION_ITEMS: {
                GetCompletionItemsRequest request = value.getGetCompletionItems();
                SessionState.ExportObject<ScriptSession> exportedConsole =
                        session.getExport(request.getConsoleId(), "consoleId");
                session.nonExport()
                        .require(exportedConsole)
                        .onError(responseObserver)
                        .submit(() -> {
                            getCompletionItems(request, exportedConsole, responseObserver);
                        });
                break;
            }
            case CLOSE_DOCUMENT: {
                CloseDocumentRequest request = value.getCloseDocument();
                PyObject completer = (PyObject) scriptSession.get().getVariable("jedi_settings");
                completer.callMethod("close_doc", request.getTextDocument().getUri());
                break;
            }
            case REQUEST_NOT_SET: {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Autocomplete command missing request");
            }
        }
    }

    private void getCompletionItems(GetCompletionItemsRequest request,
            SessionState.ExportObject<ScriptSession> exportedConsole,
            StreamObserver<AutoCompleteResponse> responseObserver) {
        final ScriptSession scriptSession = exportedConsole.get();
        try (final SafeCloseable ignored = scriptSession.getExecutionContext().open()) {

            PyObject completer = (PyObject) scriptSession.getVariable("jedi_settings");
            boolean canJedi = completer.callMethod("is_enabled").getBooleanValue();
            if (!canJedi) {
                log.trace().append("Ignoring completion request because jedi is disabled").endl();
                // send back an empty, failed response...
                safelyOnNext(responseObserver,
                        AutoCompleteResponse.newBuilder()
                                .setCompletionItems(GetCompletionItemsResponse.newBuilder()
                                        .setSuccess(false)
                                        .setRequestId(request.getRequestId()))
                                .build());
                return;
            }
            final VersionedTextDocumentIdentifier doc = request.getTextDocument();
            final Position pos = request.getPosition();
            final long startNano = System.nanoTime();

            if (log.isTraceEnabled()) {
                String text = completer.call("get_doc", doc.getUri()).getStringValue();
                log.trace().append("Completion version ").append(doc.getVersion())
                        .append(" has source code:").append(text).endl();
            }
            final PyObject results = completer.callMethod("do_completion", doc.getUri(), doc.getVersion(),
                    // our java is 0-indexed lines, 1-indexed chars. jedi is 1-indexed-both.
                    // we'll keep that translation ugliness to the in-java result-processing.
                    pos.getLine() + 1, pos.getCharacter());
            if (!results.isList()) {
                throw new UnsupportedOperationException(
                        "Expected list from jedi_settings.do_completion, got " + results.call("repr"));
            }
            final long nanosJedi = System.nanoTime();
            // translate from-python list of completion results. For now, each item in the outer list is a [str, int]
            // which contains the text of the replacement, and the column where is should be inserted.
            List<CompletionItem> finalItems = new ArrayList<>();

            for (PyObject result : results.asList()) {
                if (!result.isList()) {
                    throw new UnsupportedOperationException("Expected list-of-lists from jedi_settings.do_completion, "
                            +
                            "got bad result " + result.call("repr") + " from full results: " + results.call("repr"));
                }
                // we expect [ "completion text", start_column ] as our result.
                // in the future we may want to get more interesting info from jedi to pass back to client
                final List<PyObject> items = result.asList();
                String completionName = items.get(0).getStringValue();
                int start = items.get(1).getIntValue();
                final CompletionItem.Builder item = CompletionItem.newBuilder();
                final TextEdit.Builder textEdit = item.getTextEditBuilder();
                textEdit.setText(completionName);
                final DocumentRange.Builder range = textEdit.getRangeBuilder();
                item.setStart(start);
                item.setLabel(completionName);
                item.setLength(completionName.length());
                range.getStartBuilder().setLine(pos.getLine()).setCharacter(start);
                range.getEndBuilder().setLine(pos.getLine()).setCharacter(start + completionName.length());
                item.setInsertTextFormat(2);
                item.setSortText(ChunkerCompleter.sortable(finalItems.size()));
                finalItems.add(item.build());
            }

            final long nanosBuiltResponse = System.nanoTime();

            final GetCompletionItemsResponse builtItems = GetCompletionItemsResponse.newBuilder()
                    .setSuccess(true)
                    .setRequestId(request.getRequestId())
                    .addAllItems(finalItems)
                    .build();

            try {
                safelyOnNext(responseObserver,
                        AutoCompleteResponse.newBuilder()
                                .setCompletionItems(builtItems)
                                .build());
            } finally {
                // let's track how long completions take, as it's known that some
                // modules like numpy can cause slow completion, and we'll want to know what was causing them
                final long totalCompletionNanos = nanosBuiltResponse - startNano;
                final long totalJediNanos = nanosJedi - startNano;
                final long totalResponseBuildNanos = nanosBuiltResponse - nanosJedi;
                // only log completions taking more than 100ms
                if (totalCompletionNanos > HUNDRED_MS_IN_NS && log.isTraceEnabled()) {
                    log.trace().append("Found ")
                            .append(finalItems.size())
                            .append(" jedi completions from doc ")
                            .append(doc.getVersion())
                            .append("\tjedi_time=").append(toMillis(totalJediNanos))
                            .append("\tbuild_response_time=").append(toMillis(totalResponseBuildNanos))
                            .append("\ttotal_complete_time=").append(toMillis(totalCompletionNanos))
                            .endl();
                }
            }
        } catch (Throwable exception) {
            if (ConsoleServiceGrpcImpl.QUIET_AUTOCOMPLETE_ERRORS) {
                exception.printStackTrace();
                if (log.isTraceEnabled()) {
                    log.trace().append("Exception occurred during autocomplete").append(exception).endl();
                }
            } else {
                log.error().append("Exception occurred during autocomplete").append(exception).endl();
            }
            safelyOnNext(responseObserver,
                    AutoCompleteResponse.newBuilder()
                            .setCompletionItems(GetCompletionItemsResponse.newBuilder()
                                    .setSuccess(false)
                                    .setRequestId(request.getRequestId()))
                            .build());
            if (exception instanceof Error) {
                throw exception;
            }
        }
    }

    private String toMillis(final long totalNanos) {
        StringBuilder totalNano = new StringBuilder(Long.toString(totalNanos));
        while (totalNano.length() < 7) {
            totalNano.insert(0, "0");
        }
        int milliCutoff = totalNano.length() - 6;
        return totalNano.substring(0, milliCutoff) + "."
                + (totalNano.substring(milliCutoff, Math.min(milliCutoff + 2, totalNano.length()))) + "ms";
    }

    @Override
    public void onError(Throwable t) {
        // ignore, client doesn't need us, will be cleaned up later
    }

    @Override
    public void onCompleted() {
        // just hang up too, browser will reconnect if interested
        safelyComplete(responseObserver);
    }
}
