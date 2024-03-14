//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.completer;

import com.google.rpc.Code;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.proto.backplane.script.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.session.SessionCloseableObserver;
import io.deephaven.server.session.SessionState;
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
                final TextDocumentItem doc = value.getOpenDocument().getTextDocument();
                PyObject completer =
                        (PyObject) scriptSession.get().getQueryScope().readParamValue("jedi_settings");
                completer.callMethod("open_doc", doc.getText(), doc.getUri(), doc.getVersion());
                break;
            }
            case CHANGE_DOCUMENT: {
                ChangeDocumentRequest request = value.getChangeDocument();
                final VersionedTextDocumentIdentifier text = request.getTextDocument();

                PyObject completer =
                        (PyObject) scriptSession.get().getQueryScope().readParamValue("jedi_settings");
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
                // TODO (https://github.com/deephaven/deephaven-core/issues/3614): Add publish diagnostics
                // responseObserver.onNext(AutoCompleteResponse.newBuilder().setDiagnosticPublish());
                break;
            }
            case CLOSE_DOCUMENT: {
                PyObject completer =
                        (PyObject) scriptSession.get().getQueryScope().readParamValue("jedi_settings");
                CloseDocumentRequest request = value.getCloseDocument();
                completer.callMethod("close_doc", request.getTextDocument().getUri());
                break;
            }
            case REQUEST_NOT_SET: {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Autocomplete command missing request");
            }
            default: {
                // Maintain compatibility with older clients
                // The only autocomplete type supported before the consoleId was moved to the parent request was
                // GetCompletionItems
                final io.deephaven.proto.backplane.grpc.Ticket consoleId =
                        value.hasConsoleId() ? value.getConsoleId() : value.getGetCompletionItems().getConsoleId();
                SessionState.ExportObject<ScriptSession> exportedConsole = session.getExport(consoleId, "consoleId");
                session.nonExport()
                        .require(exportedConsole)
                        .onError(responseObserver)
                        .submit(() -> {
                            handleAutocompleteRequest(value, exportedConsole, responseObserver);
                        });
            }
        }
    }

    private void handleAutocompleteRequest(AutoCompleteRequest request,
            SessionState.ExportObject<ScriptSession> exportedConsole,
            StreamObserver<AutoCompleteResponse> responseObserver) {
        // Maintain compatibility with older clients
        // The only autocomplete type supported before the consoleId was moved to the parent request was
        // GetCompletionItems
        final int requestId =
                request.getRequestId() > 0 ? request.getRequestId() : request.getGetCompletionItems().getRequestId();
        try {
            final ScriptSession scriptSession = exportedConsole.get();
            PyObject completer = scriptSession.getQueryScope().readParamValue("jedi_settings");
            boolean canJedi = completer.callMethod("is_enabled").getBooleanValue();
            if (!canJedi) {
                log.trace().append("Ignoring completion request because jedi is disabled").endl();
                // send back an empty, failed response...
                safelyOnNext(responseObserver,
                        AutoCompleteResponse.newBuilder()
                                .setSuccess(false)
                                .setRequestId(requestId)
                                .build());
                return;
            }

            AutoCompleteResponse.Builder response = AutoCompleteResponse.newBuilder();

            switch (request.getRequestCase()) {
                case GET_COMPLETION_ITEMS: {
                    response.setCompletionItems(getCompletionItems(request.getGetCompletionItems(), completer));
                    break;
                }
                case GET_SIGNATURE_HELP: {
                    response.setSignatures(getSignatureHelp(request.getGetSignatureHelp(), completer));
                    break;
                }
                case GET_HOVER: {
                    response.setHover(getHover(request.getGetHover(), completer));
                    break;
                }
                case GET_DIAGNOSTIC: {
                    // TODO (https://github.com/deephaven/deephaven-core/issues/3614): Add user requested diagnostics
                    response.setDiagnostic(GetPullDiagnosticResponse.getDefaultInstance());
                    break;
                }
            }

            safelyOnNext(responseObserver,
                    response
                            .setSuccess(true)
                            .setRequestId(requestId)
                            .build());

        } catch (Throwable exception) {
            if (ConsoleServiceGrpcImpl.QUIET_AUTOCOMPLETE_ERRORS) {
                if (log.isTraceEnabled()) {
                    log.trace().append("Exception occurred during autocomplete").append(exception).endl();
                }
            } else {
                log.error().append("Exception occurred during autocomplete").append(exception).endl();
            }
            safelyOnNext(responseObserver,
                    AutoCompleteResponse.newBuilder()
                            .setSuccess(false)
                            .setRequestId(requestId)
                            .build());
            if (exception instanceof Error) {
                throw exception;
            }
        }
    }

    private GetCompletionItemsResponse getCompletionItems(GetCompletionItemsRequest request, PyObject completer) {
        final VersionedTextDocumentIdentifier doc = request.getTextDocument();
        final Position pos = request.getPosition();

        final PyObject results = completer.callMethod("do_completion", doc.getUri(), doc.getVersion(),
                // our java is 0-indexed lines and chars. jedi is 1-indexed lines and 0-indexed chars
                // we'll keep that translation ugliness to the in-java result-processing.
                pos.getLine() + 1, pos.getCharacter());
        if (!results.isList()) {
            throw new UnsupportedOperationException(
                    "Expected list from jedi_settings.do_completion, got " + results.call("repr"));
        }
        // translate from-python list of completion results. For now, each item in the outer list is a [str, int]
        // which contains the text of the replacement, and the column where it should be inserted.
        List<CompletionItem> finalItems = new ArrayList<>();

        for (PyObject result : results.asList()) {
            if (!result.isList()) {
                throw new UnsupportedOperationException("Expected list-of-lists from jedi_settings.do_completion, "
                        +
                        "got bad result " + result.call("repr") + " from full results: " + results.call("repr"));
            }
            // we expect [ "completion text", start_column, description, docstring, kind ] as our result.
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
            item.setDetail(items.get(2).getStringValue());
            item.setDocumentation(
                    MarkupContent.newBuilder().setValue(items.get(3).getStringValue()).setKind("plaintext").build());
            item.setKind(items.get(4).getIntValue());
            range.getStartBuilder().setLine(pos.getLine()).setCharacter(start);
            range.getEndBuilder().setLine(pos.getLine()).setCharacter(pos.getCharacter());
            item.setInsertTextFormat(2);
            item.setSortText(ChunkerCompleter.sortable(finalItems.size()));
            finalItems.add(item.build());
        }

        return GetCompletionItemsResponse.newBuilder()
                .setSuccess(true)
                .setRequestId(request.getRequestId())
                .addAllItems(finalItems)
                .build();
    }

    private GetSignatureHelpResponse getSignatureHelp(GetSignatureHelpRequest request, PyObject completer) {
        final VersionedTextDocumentIdentifier doc = request.getTextDocument();
        final Position pos = request.getPosition();

        final PyObject results = completer.callMethod("do_signature_help", doc.getUri(), doc.getVersion(),
                // our java is 0-indexed lines and chars. jedi is 1-indexed lines and 0-indexed chars
                // we'll keep that translation ugliness to the in-java result-processing.
                pos.getLine() + 1, pos.getCharacter());
        if (!results.isList()) {
            throw new UnsupportedOperationException(
                    "Expected list from jedi_settings.do_signature_help, got " + results.call("repr"));
        }

        // translate from-python list of completion results. For now, each item in the outer list is a [str, int]
        // which contains the text of the replacement, and the column where is should be inserted.
        List<SignatureInformation> finalItems = new ArrayList<>();

        for (PyObject result : results.asList()) {
            if (!result.isList()) {
                throw new UnsupportedOperationException("Expected list-of-lists from jedi_settings.do_signature_help, "
                        +
                        "got bad result " + result.call("repr") + " from full results: " + results.call("repr"));
            }
            // we expect [ label, documentation, [params], active_parameter ] as our result
            final List<PyObject> signature = result.asList();
            String label = signature.get(0).getStringValue();
            String docstring = signature.get(1).getStringValue();
            int activeParam = signature.get(3).getIntValue();

            final SignatureInformation.Builder item = SignatureInformation.newBuilder();
            item.setLabel(label);
            item.setDocumentation(MarkupContent.newBuilder().setValue(docstring).setKind("plaintext").build());
            item.setActiveParameter(activeParam);

            signature.get(2).asList().forEach(obj -> {
                final List<PyObject> param = obj.asList();
                item.addParameters(ParameterInformation.newBuilder().setLabel(param.get(0).getStringValue())
                        .setDocumentation(MarkupContent.newBuilder().setValue(param.get(1).getStringValue())
                                .setKind("plaintext").build()));
            });

            finalItems.add(item.build());
        }

        return GetSignatureHelpResponse.newBuilder()
                .addAllSignatures(finalItems)
                .build();
    }

    private GetHoverResponse getHover(GetHoverRequest request, PyObject completer) {
        final VersionedTextDocumentIdentifier doc = request.getTextDocument();
        final Position pos = request.getPosition();

        final PyObject result = completer.callMethod("do_hover", doc.getUri(), doc.getVersion(),
                // our java is 0-indexed lines and chars. jedi is 1-indexed lines and 0-indexed chars
                // we'll keep that translation ugliness to the in-java result-processing.
                pos.getLine() + 1, pos.getCharacter());
        if (!result.isString()) {
            throw new UnsupportedOperationException(
                    "Expected string from jedi_settings.do_hover, got " + result.call("repr"));
        }

        // We don't set the range b/c Jedi doesn't seem to give the word range under the cursor easily
        // Monaco in the web auto-detects the word range for the hover if not set
        return GetHoverResponse.newBuilder()
                .setContents(MarkupContent.newBuilder().setValue(result.getStringValue()).setKind("markdown"))
                .build();
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
