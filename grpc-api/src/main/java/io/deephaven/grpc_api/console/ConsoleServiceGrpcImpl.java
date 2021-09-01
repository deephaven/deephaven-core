/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.console;

import com.google.rpc.Code;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.plot.FigureWidget;
import io.deephaven.db.tables.Table;
import io.deephaven.db.util.DelegatingScriptSession;
import io.deephaven.db.util.ExportedObjectType;
import io.deephaven.db.util.NoLanguageDeephavenSession;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.db.util.VariableProvider;
import io.deephaven.db.v2.DynamicNode;
import io.deephaven.figures.FigureWidgetTranslator;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.SessionState.ExportBuilder;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferRecord;
import io.deephaven.io.logger.LogBufferRecordListener;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionLookups;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.lang.parse.LspTools;
import io.deephaven.lang.parse.ParsedDocument;
import io.deephaven.lang.shared.lsp.CompletionCancelled;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.script.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecute;
import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecuteLocked;

@Singleton
public class ConsoleServiceGrpcImpl extends ConsoleServiceGrpc.ConsoleServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ConsoleServiceGrpcImpl.class);

    public static final String WORKER_CONSOLE_TYPE =
            Configuration.getInstance().getStringWithDefault("deephaven.console.type", "python");
    public static final boolean REMOTE_CONSOLE_DISABLED =
            Configuration.getInstance().getBooleanWithDefault("deephaven.console.disable", false);

    private final Map<String, Provider<ScriptSession>> scriptTypes;
    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final LogBuffer logBuffer;

    private final Map<SessionState, CompletionParser> parsers = new ConcurrentHashMap<>();

    private final GlobalSessionProvider globalSessionProvider;

    @Inject
    public ConsoleServiceGrpcImpl(final Map<String, Provider<ScriptSession>> scriptTypes,
            final TicketRouter ticketRouter,
            final SessionService sessionService,
            final LogBuffer logBuffer,
            final GlobalSessionProvider globalSessionProvider) {
        this.scriptTypes = scriptTypes;
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.logBuffer = logBuffer;
        this.globalSessionProvider = globalSessionProvider;

        if (!scriptTypes.containsKey(WORKER_CONSOLE_TYPE)) {
            throw new IllegalArgumentException("Console type not found: " + WORKER_CONSOLE_TYPE);
        }
    }

    public void initializeGlobalScriptSession() {
        globalSessionProvider.initializeGlobalScriptSession(scriptTypes.get(WORKER_CONSOLE_TYPE).get());
    }

    @Override
    public void getConsoleTypes(final GetConsoleTypesRequest request,
            final StreamObserver<GetConsoleTypesResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            if (!REMOTE_CONSOLE_DISABLED) {
                // TODO (#702): initially show all console types; the first console determines the global console type
                // thereafter
                responseObserver.onNext(GetConsoleTypesResponse.newBuilder()
                        .addConsoleTypes(WORKER_CONSOLE_TYPE)
                        .build());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void startConsole(StartConsoleRequest request, StreamObserver<StartConsoleResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            SessionState session = sessionService.getCurrentSession();
            if (REMOTE_CONSOLE_DISABLED) {
                responseObserver
                        .onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Remote console disabled"));
                return;
            }

            // TODO auth hook, ensure the user can do this (owner of worker or admin)
            // session.getAuthContext().requirePrivilege(CreateConsole);

            // TODO (#702): initially global session will be null; set it here if applicable

            final String sessionType = request.getSessionType();
            if (!scriptTypes.containsKey(sessionType)) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "session type '" + sessionType + "' is not supported");
            }

            session.newExport(request.getResultId(), "resultId")
                    .onError(responseObserver)
                    .submit(() -> {
                        final ScriptSession scriptSession;
                        if (sessionType.equals(WORKER_CONSOLE_TYPE)) {
                            scriptSession = new DelegatingScriptSession(globalSessionProvider.getGlobalSession());
                        } else {
                            scriptSession = new NoLanguageDeephavenSession(sessionType);
                            log.error().append("Session type '" + sessionType + "' is disabled." +
                                    "Use the session type '" + WORKER_CONSOLE_TYPE + "' instead.").endl();
                        }

                        safelyExecute(() -> {
                            responseObserver.onNext(StartConsoleResponse.newBuilder()
                                    .setResultId(request.getResultId())
                                    .build());
                            responseObserver.onCompleted();
                        });

                        return scriptSession;
                    });
        });
    }

    @Override
    public void subscribeToLogs(LogSubscriptionRequest request, StreamObserver<LogSubscriptionData> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            if (REMOTE_CONSOLE_DISABLED) {
                responseObserver
                        .onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Remote console disabled"));
                return;
            }
            SessionState session = sessionService.getCurrentSession();
            // if that didn't fail, we at least are authenticated, but possibly not authorized
            // TODO auth hook, ensure the user can do this (owner of worker or admin). same rights as creating a console
            // session.getAuthContext().requirePrivilege(LogBuffer);

            logBuffer.subscribe(new LogBufferStreamAdapter(session, request, responseObserver));
        });
    }

    @Override
    public void executeCommand(ExecuteCommandRequest request, StreamObserver<ExecuteCommandResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final Ticket consoleId = request.getConsoleId();
            if (consoleId.getTicket().isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No consoleId supplied");
            }

            SessionState.ExportObject<ScriptSession> exportedConsole =
                    ticketRouter.resolve(session, consoleId, "consoleId");
            session.nonExport()
                    .requiresSerialQueue()
                    .require(exportedConsole)
                    .onError(responseObserver)
                    .submit(() -> {
                        ScriptSession scriptSession = exportedConsole.get();

                        // produce a diff
                        ExecuteCommandResponse.Builder diff = ExecuteCommandResponse.newBuilder();

                        ScriptSession.Changes changes = scriptSession.evaluateScript(request.getCode());

                        changes.created.entrySet()
                                .forEach(entry -> diff.addCreated(makeVariableDefinition(entry)));
                        changes.updated.entrySet()
                                .forEach(entry -> diff.addUpdated(makeVariableDefinition(entry)));
                        changes.removed.entrySet()
                                .forEach(entry -> diff.addRemoved(makeVariableDefinition(entry)));

                        responseObserver.onNext(diff.build());
                        responseObserver.onCompleted();
                    });
        });
    }

    private static VariableDefinition makeVariableDefinition(Map.Entry<String, ExportedObjectType> entry) {
        return VariableDefinition.newBuilder().setTitle(entry.getKey()).setType(entry.getValue().name())
                .setId(ScopeTicketResolver.ticketForName(entry.getKey())).build();
    }

    @Override
    public void cancelCommand(CancelCommandRequest request, StreamObserver<CancelCommandResponse> responseObserver) {
        // TODO not yet implemented, need a way to handle stopping a command in a consistent way
        super.cancelCommand(request, responseObserver);
    }

    @Override
    public void bindTableToVariable(BindTableToVariableRequest request,
            StreamObserver<BindTableToVariableResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            Ticket tableId = request.getTableId();
            if (tableId.getTicket().isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No source tableId supplied");
            }
            final SessionState.ExportObject<Table> exportedTable = ticketRouter.resolve(session, tableId, "tableId");
            final SessionState.ExportObject<ScriptSession> exportedConsole;

            ExportBuilder<?> exportBuilder = session.nonExport()
                    .requiresSerialQueue()
                    .onError(responseObserver);

            if (request.hasConsoleId()) {
                exportedConsole = ticketRouter.resolve(session, request.getConsoleId(), "consoleId");
                exportBuilder.require(exportedTable, exportedConsole);
            } else {
                exportedConsole = null;
                exportBuilder.require(exportedTable);
            }

            exportBuilder.submit(() -> {
                ScriptSession scriptSession =
                        exportedConsole != null ? exportedConsole.get() : globalSessionProvider.getGlobalSession();
                Table table = exportedTable.get();
                scriptSession.setVariable(request.getVariableName(), table);
                if (DynamicNode.notDynamicOrIsRefreshing(table)) {
                    scriptSession.manage(table);
                }
                responseObserver.onNext(BindTableToVariableResponse.getDefaultInstance());
                responseObserver.onCompleted();
            });
        });
    }

    private CompletionParser ensureParserForSession(SessionState session) {
        return parsers.computeIfAbsent(session, s -> {
            CompletionParser parser = new CompletionParser();
            s.addOnCloseCallback(() -> {
                parsers.remove(s);
                parser.close();
            });
            return parser;
        });
    }

    @Override
    public StreamObserver<AutoCompleteRequest> autoCompleteStream(
            StreamObserver<AutoCompleteResponse> responseObserver) {
        return GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            CompletionParser parser = ensureParserForSession(session);
            return new StreamObserver<AutoCompleteRequest>() {

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
                            parser.update(text.getUri(), Integer.toString(text.getVersion()),
                                    request.getContentChangesList());
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
                                        final VersionedTextDocumentIdentifier doc = request.getTextDocument();
                                        ScriptSession scriptSession = exportedConsole.get();
                                        final VariableProvider vars = scriptSession.getVariableProvider();
                                        final CompletionLookups h = CompletionLookups.preload(scriptSession);
                                        // The only stateful part of a completer is the CompletionLookups, which are
                                        // already once-per-session-cached
                                        // so, we'll just create a new completer for each request. No need to hang onto
                                        // these guys.
                                        final ChunkerCompleter completer = new ChunkerCompleter(log, vars, h);

                                        final ParsedDocument parsed;
                                        try {
                                            parsed = parser.finish(doc.getUri());
                                        } catch (CompletionCancelled exception) {
                                            if (log.isTraceEnabled()) {
                                                log.trace().append("Completion canceled").append(exception).endl();
                                            }
                                            safelyExecuteLocked(responseObserver,
                                                    () -> responseObserver.onNext(AutoCompleteResponse.newBuilder()
                                                            .setCompletionItems(GetCompletionItemsResponse.newBuilder()
                                                                    .setSuccess(false)
                                                                    .setRequestId(request.getRequestId()))
                                                            .build()));
                                            return;
                                        }

                                        int offset = LspTools.getOffsetFromPosition(parsed.getSource(),
                                                request.getPosition());
                                        final Collection<CompletionItem.Builder> results =
                                                completer.runCompletion(parsed, request.getPosition(), offset);
                                        final GetCompletionItemsResponse mangledResults =
                                                GetCompletionItemsResponse.newBuilder()
                                                        .setSuccess(true)
                                                        .setRequestId(request.getRequestId())
                                                        .addAllItems(results.stream().map(
                                                                // insertTextFormat is a default we used to set in
                                                                // constructor;
                                                                // for now, we'll just process the objects before
                                                                // sending back to client
                                                                item -> item.setInsertTextFormat(2).build())
                                                                .collect(Collectors.toSet()))
                                                        .build();

                                        safelyExecuteLocked(responseObserver,
                                                () -> responseObserver.onNext(AutoCompleteResponse.newBuilder()
                                                        .setCompletionItems(mangledResults)
                                                        .build()));
                                    });
                            break;
                        }
                        case CLOSE_DOCUMENT: {
                            CloseDocumentRequest request = value.getCloseDocument();
                            parser.remove(request.getTextDocument().getUri());
                            break;
                        }
                        case REQUEST_NOT_SET: {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "Autocomplete command missing request");
                        }
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
            };
        });
    }

    @Override
    public void fetchFigure(FetchFigureRequest request, StreamObserver<FetchFigureResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            if (request.getSourceId().getTicket().isEmpty()) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No sourceId supplied");
            }
            final SessionState.ExportObject<Object> figure = ticketRouter.resolve(
                    session, request.getSourceId(), "sourceId");

            session.nonExport()
                    .require(figure)
                    .onError(responseObserver)
                    .submit(() -> {
                        Object result = figure.get();
                        if (!(result instanceof FigureWidget)) {
                            final String name = ticketRouter.getLogNameFor(request.getSourceId(), "sourceId");
                            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                                    "Value bound to ticket " + name + " is not a FigureWidget");
                        }
                        FigureWidget widget = (FigureWidget) result;

                        FigureDescriptor translated = FigureWidgetTranslator.translate(widget, session);

                        responseObserver
                                .onNext(FetchFigureResponse.newBuilder().setFigureDescriptor(translated).build());
                        responseObserver.onCompleted();
                    });
        });
    }

    private class LogBufferStreamAdapter implements Closeable, LogBufferRecordListener {
        private final SessionState session;
        private final LogSubscriptionRequest request;
        private final StreamObserver<LogSubscriptionData> responseObserver;
        private boolean isClosed = false;

        public LogBufferStreamAdapter(
                final SessionState session,
                final LogSubscriptionRequest request,
                final StreamObserver<LogSubscriptionData> responseObserver) {
            this.session = session;
            this.request = request;
            this.responseObserver = responseObserver;
            session.addOnCloseCallback(this);
            ((ServerCallStreamObserver<LogSubscriptionData>) responseObserver).setOnCancelHandler(this::tryClose);
        }

        @Override
        public void close() {
            synchronized (this) {
                if (isClosed) {
                    return;
                }
                isClosed = true;
            }

            safelyExecute(() -> logBuffer.unsubscribe(this));
            safelyExecuteLocked(responseObserver, responseObserver::onCompleted);
        }

        private void tryClose() {
            if (session.removeOnCloseCallback(this)) {
                close();
            }
        }

        @Override
        public void record(LogBufferRecord record) {
            // only pass levels the client wants
            if (request.getLevelsCount() != 0 && !request.getLevelsList().contains(record.getLevel().getName())) {
                return;
            }

            // since the subscribe() method auto-replays all existing logs, filter to just once this
            // consumer probably hasn't seen
            if (record.getTimestampMicros() < request.getLastSeenLogTimestamp()) {
                return;
            }

            // TODO this is not a good implementation, just a quick one, but it does appear to be safe,
            // since LogBuffer is synchronized on access to the listeners. We're on the same thread
            // as all other log receivers and
            try {
                LogSubscriptionData payload = LogSubscriptionData.newBuilder()
                        .setMicros(record.getTimestampMicros())
                        .setLogLevel(record.getLevel().getName())
                        // this could be done on either side, doing it here because its a weird charset and we should
                        // own that
                        .setMessage(record.getDataString())
                        .build();
                synchronized (responseObserver) {
                    responseObserver.onNext(payload);
                }
            } catch (Throwable t) {
                // we are ignoring exceptions here deliberately, and just shutting down
                tryClose();
            }
        }
    }
}
