package io.deephaven.grpc_api.console;

import com.google.rpc.Code;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.db.util.VariableProvider;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.browserstreaming.BrowserStream;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionLookups;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.lang.parse.LspTools;
import io.deephaven.lang.parse.ParsedDocument;
import io.deephaven.proto.backplane.script.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.Collection;
import java.util.stream.Collectors;

import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecute;

public class AutocompleteMessageMarshaller implements BrowserStream.Marshaller<AutoCompleteRequest>, StreamObserver<AutoCompleteRequest> {
    private static final Logger log = LoggerFactory.getLogger(AutocompleteMessageMarshaller.class);

    private final StreamObserver<AutoCompleteResponse> observer;
    private final SessionState session;
    private final CompletionParser parser;

    public AutocompleteMessageMarshaller(StreamObserver<AutoCompleteResponse> observer, SessionState session, CompletionParser parser) {
        this.observer = observer;
        this.session = session;
        this.parser = parser;
    }

    @Override
    public void onNext(final AutoCompleteRequest request) {
        GrpcUtil.rpcWrapper(log, observer, () -> {
            onMessageReceived(request);
        });
    }

    @Override
    public void onMessageReceived(AutoCompleteRequest message) {
        switch (message.getRequestCase()) {
            case OPEN_DOCUMENT: {
                final TextDocumentItem doc = message.getOpenDocument().getTextDocument();

                parser.open(doc.getText(), doc.getUri(), Integer.toString(doc.getVersion()));
                break;
            }
            case CHANGE_DOCUMENT: {
                ChangeDocumentRequest request = message.getChangeDocument();
                final VersionedTextDocumentIdentifier text = request.getTextDocument();
                parser.update(text.getUri(), Integer.toString(text.getVersion()), request.getContentChangesList());
                break;
            }
            case GET_COMPLETION_ITEMS: {
                GetCompletionItemsRequest request = message.getGetCompletionItems();
                SessionState.ExportObject<ScriptSession> exportedConsole = session.getExport(request.getConsoleId());
                session.nonExport()
                        .require(exportedConsole)
                        .onError(observer::onError)
                        .submit(() -> {
                            final VersionedTextDocumentIdentifier doc = request.getTextDocument();
                            ScriptSession scriptSession = exportedConsole.get();
                            final VariableProvider vars = scriptSession.getVariableProvider();
                            final CompletionLookups h = CompletionLookups.preload(scriptSession);
                            // The only stateful part of a completer is the CompletionLookups, which are already once-per-session-cached
                            // so, we'll just create a new completer for each request. No need to hand onto these guys.
                            final ChunkerCompleter completer = new ChunkerCompleter(log, vars, h);
                            final ParsedDocument parsed = parser.finish(doc.getUri());
                            int offset = LspTools.getOffsetFromPosition(parsed.getSource(), request.getPosition());
                            final Collection<CompletionItem.Builder> results = completer.runCompletion(parsed, request.getPosition(), offset);
                            final GetCompletionItemsResponse mangledResults = GetCompletionItemsResponse.newBuilder()
                                    .addAllItems(results.stream().map(
                                            // insertTextFormat is a default we used to set in constructor;
                                            // for now, we'll just process the objects before sending back to client
                                            item -> item.setInsertTextFormat(2).build()
                                    ).collect(Collectors.toSet())).build();

                            safelyExecute(() -> {
                                observer.onNext(AutoCompleteResponse.newBuilder()
                                        .setCompletionItems(mangledResults)
                                        .build());
                            });
                        });
                break;
            }
            case CLOSE_DOCUMENT: {
                CloseDocumentRequest request = message.getCloseDocument();
                parser.remove(request.getTextDocument().getUri());
                break;
            }
            case REQUEST_NOT_SET: {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Autocomplete command missing request");
            }
        }
    }

    @Override
    public void onCancel() {
        // ignore, client doesn't need us, will be cleaned up later
    }

    @Override
    public void onError(Throwable err) {
        // ignore, client doesn't need us, will be cleaned up later
    }

    @Override
    public void onCompleted() {
        // just hang up too, browser will reconnect if interested
        observer.onCompleted();
    }
}
