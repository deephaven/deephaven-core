package io.deephaven.server.console.completer;

import com.google.rpc.Code;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.engine.util.VariableProvider;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionLookups;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.lang.parse.LspTools;
import io.deephaven.lang.parse.ParsedDocument;
import io.deephaven.lang.shared.lsp.CompletionCancelled;
import io.deephaven.proto.backplane.script.grpc.*;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jpy.PyListWrapper;
import org.jpy.PyObject;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyExecuteLocked;

/**
 * Autocomplete handling for python that will use the jedi library, if it is installed.
 */
public class PythonAutoCompleteObserver implements StreamObserver<AutoCompleteRequest> {

    private static final Logger log = LoggerFactory.getLogger(PythonAutoCompleteObserver.class);
    private final Provider<ScriptSession> scriptSession;
    private final SessionState session;
    private boolean canJedi, checkedJedi;
    private final StreamObserver<AutoCompleteResponse> responseObserver;

    public PythonAutoCompleteObserver(StreamObserver<AutoCompleteResponse> responseObserver, Provider<ScriptSession> scriptSession, final SessionState session) {
        this.scriptSession = scriptSession;
        this.session = session;
        this.responseObserver = responseObserver;
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

                final List<ChangeDocumentRequest.TextDocumentContentChangeEvent> changes = request.getContentChangesList();
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
            final VariableProvider vars = scriptSession.getVariableProvider();

            PyObject completer = (PyObject) scriptSession.getVariable("jedi_settings");
            boolean canJedi = completer.callMethod("is_enabled").getBooleanValue();
            if (!canJedi) {
                log.trace().append("Ignoring completion request because jedi is disabled").endl();
                return;
            }
            final VersionedTextDocumentIdentifier doc = request.getTextDocument();
            final long startNano = System.nanoTime();
            String text = completer.call("get_doc", doc.getUri()).getStringValue();
            try {
                final Position pos = request.getPosition();
                String completionVar = "completions";
                scriptSession.setVariable("__jedi_source__", text);
                final ScriptSession.Changes changes = scriptSession.evaluateScript(
                        completionVar + " = jedi.Interpreter(jedi_settings.get_doc('" + doc.getUri() + "'), [globals()]).complete(" +
                                (pos.getLine() + 1) + "," + pos.getCharacter() + ")"
                );
                final PyListWrapper completes = vars.getVariable(completionVar, null);
                List<CompletionItem.Builder> completionResults = new ArrayList<>();
                List<CompletionItem.Builder> completionResults_ = new ArrayList<>();
                List<CompletionItem.Builder> completionResults__ = new ArrayList<>();
                for (PyObject completion : completes) {
                    String completionName = completion.getAttribute("name").getStringValue();
                    int completionPrefix = completion.call("get_completion_prefix_length").getIntValue();
                    final CompletionItem.Builder item = CompletionItem.newBuilder();
                    final TextEdit.Builder textEdit = item.getTextEditBuilder();
                    textEdit.setText(completionName);
                    final DocumentRange.Builder range = textEdit.getRangeBuilder();
                    int start = pos.getCharacter() - completionPrefix;
                    item.setStart(start);
                    item.setLabel(completionName);
                    item.setLength(completionName.length());
                    range.getStartBuilder().setLine(pos.getLine()).setCharacter(start);
                    range.getEndBuilder().setLine(pos.getLine()).setCharacter(start + completionName.length());
                    item.setInsertTextFormat(2);
                    if (completionName.startsWith("__")) {
                        completionResults__.add(item);
                    } else if (completionName.startsWith("_")) {
                        completionResults_.add(item);
                    } else {
                        completionResults.add(item);
                    }
                }
                int sortPos = 0;
                List<CompletionItem> finalItems = new ArrayList<>();
                for (CompletionItem.Builder res : completionResults) {
                    res.setSortText(ChunkerCompleter.sortable(sortPos++));
                    finalItems.add(res.build());
                }
                for (CompletionItem.Builder res : completionResults_) {
                    res.setSortText(ChunkerCompleter.sortable(sortPos++));
                    finalItems.add(res.build());
                }
                for (CompletionItem.Builder res : completionResults__) {
                    res.setSortText(ChunkerCompleter.sortable(sortPos++));
                    finalItems.add(res.build());
                }

                final GetCompletionItemsResponse builtItems = GetCompletionItemsResponse.newBuilder()
                        .setSuccess(true)
                        .setRequestId(request.getRequestId())
                        .addAllItems(finalItems)
                        .build();

                safelyExecuteLocked(responseObserver,
                        () -> responseObserver.onNext(AutoCompleteResponse.newBuilder()
                                .setCompletionItems(builtItems)
                                .build()));
                String totalNano = Long.toString(System.nanoTime() - startNano);
                // lets track how long completions take, as it's known that some
                // modules like numpy can cause slow completion, and we'll want to know what was causing them
                log.info().append("Jedi completions took ")
                        .append(
                                totalNano.length() > 6 ?
                                        totalNano.substring(0, totalNano.length() - 6) :
                                        "0"
                        )
                        .append('.')
                        .append(totalNano.substring(6, Math.min(8, totalNano.length())))
                        .append("ms").endl();

            } catch (Throwable e) {
                log.error().append("Jedi completion failure ").append(e).endl();
            }
        } catch (Exception exception) {
            if (ConsoleServiceGrpcImpl.QUIET_AUTOCOMPLETE_ERRORS) {
                if (log.isTraceEnabled()) {
                    log.trace().append("Exception occurred during autocomplete").append(exception).endl();
                }
            } else {
                log.error().append("Exception occurred during autocomplete").append(exception).endl();
            }
            safelyExecuteLocked(responseObserver,
                    () -> responseObserver.onNext(AutoCompleteResponse.newBuilder()
                            .setCompletionItems(GetCompletionItemsResponse.newBuilder()
                                    .setSuccess(false)
                                    .setRequestId(request.getRequestId()))
                            .build()));
        }
    }

    @Override
    public void onError(Throwable t) {
        // ignore, client doesn't need us, will be cleaned up later
    }

    @Override
    public void onCompleted() {
        // just hang up too, browser will reconnect if interested
        synchronized (responseObserver) {
            responseObserver.onCompleted();
        }
    }
}
