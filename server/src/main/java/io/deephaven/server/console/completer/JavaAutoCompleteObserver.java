//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console.completer;

import com.google.rpc.Code;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionLookups;
import io.deephaven.lang.completion.CustomCompletion;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.lang.parse.LspTools;
import io.deephaven.lang.parse.ParsedDocument;
import io.deephaven.lang.shared.lsp.CompletionCancelled;
import io.deephaven.proto.backplane.script.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.session.SessionCloseableObserver;
import io.deephaven.server.session.SessionState;
import io.grpc.stub.StreamObserver;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.stream.Collectors;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyOnNext;

/**
 * Autocomplete handling for JVM languages, that directly can interact with Java instances without any name mangling,
 * and are able to use our flexible parser.
 */
public class JavaAutoCompleteObserver extends SessionCloseableObserver<AutoCompleteResponse>
        implements StreamObserver<AutoCompleteRequest> {

    private static final Logger log = LoggerFactory.getLogger(JavaAutoCompleteObserver.class);
    /** Track parsers by their session state, to ensure each session has its own, singleton, parser */
    private static final Map<SessionState, CompletionParser> parsers = Collections.synchronizedMap(new WeakHashMap<>());

    private final CompletionParser parser;
    private final Set<CustomCompletion.Factory> customCompletionFactory;

    private static CompletionParser ensureParserForSession(SessionState session) {
        return parsers.computeIfAbsent(session, s -> {
            CompletionParser parser = new CompletionParser();
            s.addOnCloseCallback(() -> {
                parsers.remove(s);
                parser.close();
            });
            return parser;
        });
    }

    public JavaAutoCompleteObserver(SessionState session, StreamObserver<AutoCompleteResponse> responseObserver,
            Set<CustomCompletion.Factory> customCompletionFactory) {
        super(session, responseObserver);
        parser = ensureParserForSession(session);
        this.customCompletionFactory = customCompletionFactory;
    }

    @Override
    public void onNext(AutoCompleteRequest value) {
        switch (value.getRequestCase()) {
            case OPEN_DOCUMENT: {
                final TextDocumentItem doc = value.getOpenDocument().getTextDocument();

                parser.open(doc.getText(), doc.getUri(), Integer.toString(doc.getVersion()));
                break;
            }
            case CHANGE_DOCUMENT: {
                ChangeDocumentRequest request = value.getChangeDocument();
                final VersionedTextDocumentIdentifier text = request.getTextDocument();
                parser.update(text.getUri(), text.getVersion(),
                        request.getContentChangesList());
                break;
            }
            case CLOSE_DOCUMENT: {
                CloseDocumentRequest request = value.getCloseDocument();
                parser.remove(request.getTextDocument().getUri());
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
            AutoCompleteResponse.Builder response = AutoCompleteResponse.newBuilder();

            switch (request.getRequestCase()) {
                case GET_COMPLETION_ITEMS: {
                    response.setCompletionItems(getCompletionItems(request.getGetCompletionItems(), exportedConsole,
                            parser, responseObserver));
                    break;
                }
                case GET_SIGNATURE_HELP: {
                    // Not implemented. Return empty signature list
                    response.setSignatures(GetSignatureHelpResponse.getDefaultInstance());
                    break;
                }
                case GET_HOVER: {
                    // Not implemented. Return empty hover help
                    response.setHover(
                            GetHoverResponse.newBuilder().setContents(MarkupContent.getDefaultInstance()).build());
                    break;
                }
                case GET_DIAGNOSTIC: {
                    // Not implemented. Return empty diagnostics
                    response.setDiagnostic(GetPullDiagnosticResponse.getDefaultInstance());
                    break;
                }
            }

            safelyOnNext(responseObserver,
                    response
                            .setSuccess(true)
                            .setRequestId(requestId)
                            .build());

        } catch (CompletionCancelled exception) {
            if (log.isTraceEnabled()) {
                log.trace().append("Completion canceled").append(exception).endl();
            }
            safelyOnNext(responseObserver,
                    AutoCompleteResponse.newBuilder()
                            .setSuccess(false)
                            .setRequestId(request.getRequestId())
                            .build());
        } catch (Exception exception) {
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
                            .setRequestId(request.getRequestId())
                            .build());
        }
    }

    private GetCompletionItemsResponse getCompletionItems(GetCompletionItemsRequest request,
            SessionState.ExportObject<ScriptSession> exportedConsole, CompletionParser parser,
            StreamObserver<AutoCompleteResponse> responseObserver) {
        final ScriptSession scriptSession = exportedConsole.get();
        final QueryScope vars = scriptSession.getQueryScope();
        final VersionedTextDocumentIdentifier doc = request.getTextDocument();
        final CompletionLookups h = CompletionLookups.preload(scriptSession, customCompletionFactory);
        // The only stateful part of a completer is the CompletionLookups, which are already
        // once-per-session-cached
        // so, we'll just create a new completer for each request. No need to hang onto these guys.
        final ChunkerCompleter completer = new ChunkerCompleter(log, vars, h);

        final ParsedDocument parsed = parser.finish(doc.getUri());

        int offset = LspTools.getOffsetFromPosition(parsed.getSource(),
                request.getPosition());
        final Collection<CompletionItem.Builder> results =
                completer.runCompletion(parsed, request.getPosition(), offset);
        return GetCompletionItemsResponse.newBuilder()
                .setSuccess(true)
                .setRequestId(request.getRequestId())
                .addAllItems(results.stream().map(
                        // insertTextFormat is a default we used to set in constructor; for now, we'll
                        // just process the objects before sending back to client
                        item -> item.setInsertTextFormat(2).build())
                        .collect(Collectors.toSet()))
                .build();
    }

    @Override
    public void onError(Throwable t) {
        // ignore, client doesn't need us, will be cleaned up later
    }

    @Override
    public void onCompleted() {
        // just hang up too, browser will reconnect if interested, and we'll maintain state if the session isn't gc'd
        safelyComplete(responseObserver);
    }
}
