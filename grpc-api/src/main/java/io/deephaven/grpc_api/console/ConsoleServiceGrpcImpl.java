/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.console;

import com.google.rpc.Code;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.plot.FigureWidget;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.remote.preview.ColumnPreviewManager;
import io.deephaven.db.util.ExportedObjectType;
import io.deephaven.db.util.NoLanguageDeephavenSession;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.figures.FigureWidgetTranslator;
import io.deephaven.grpc_api.browserstreaming.BrowserStreamInterceptor;
import io.deephaven.grpc_api.browserstreaming.StreamData;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.SessionState.ExportBuilder;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.table.TableServiceGrpcImpl;
import io.deephaven.grpc_api.browserstreaming.BrowserStream;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferRecord;
import io.deephaven.io.logger.LogBufferRecordListener;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.parse.CompletionParser;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.script.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecute;
import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecuteLocked;

@Singleton
public class ConsoleServiceGrpcImpl extends ConsoleServiceGrpc.ConsoleServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ConsoleServiceGrpcImpl.class);

    public static final String WORKER_CONSOLE_TYPE = Configuration.getInstance().getStringWithDefault("io.deephaven.console.type", "python");

    private final Map<String, Provider<ScriptSession>> scriptTypes;
    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final LogBuffer logBuffer;
    private final LiveTableMonitor liveTableMonitor;

    private final Map<SessionState, CompletionParser> parsers = new ConcurrentHashMap<>();

    private final GlobalSessionProvider globalSessionProvider;

    @Inject
    public ConsoleServiceGrpcImpl(final Map<String, Provider<ScriptSession>> scriptTypes,
                                  final TicketRouter ticketRouter,
                                  final SessionService sessionService,
                                  final LogBuffer logBuffer,
                                  final LiveTableMonitor liveTableMonitor,
                                  final GlobalSessionProvider globalSessionProvider) {
        this.scriptTypes = scriptTypes;
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.logBuffer = logBuffer;
        this.liveTableMonitor = liveTableMonitor;
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
            // TODO (#702): initially show all console types; the first console determines the global console type thereafter
            responseObserver.onNext(GetConsoleTypesResponse.newBuilder()
                    .addConsoleTypes(WORKER_CONSOLE_TYPE)
                    .build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void startConsole(StartConsoleRequest request, StreamObserver<StartConsoleResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            SessionState session = sessionService.getCurrentSession();
            // TODO auth hook, ensure the user can do this (owner of worker or admin)
//            session.getAuthContext().requirePrivilege(CreateConsole);

            // TODO (#702): initially global session will be null; set it here if applicable

            final String sessionType = request.getSessionType();
            if (!scriptTypes.containsKey(sessionType)) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "session type '" + sessionType + "' is not supported");
            }

            session.newExport(request.getResultId())
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        final ScriptSession scriptSession;
                        if (sessionType.equals(WORKER_CONSOLE_TYPE)) {
                            scriptSession = globalSessionProvider.getGlobalSession();
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
            SessionState session = sessionService.getCurrentSession();
            // if that didn't fail, we at least are authenticated, but possibly not authorized
            // TODO auth hook, ensure the user can do this (owner of worker or admin). same rights as creating a console
//            session.getAuthContext().requirePrivilege(LogBuffer);

            logBuffer.subscribe(new LogBufferStreamAdapter(session, request, responseObserver));
        });
    }

    @Override
    public void executeCommand(ExecuteCommandRequest request, StreamObserver<ExecuteCommandResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<ScriptSession> exportedConsole = ticketRouter.resolve(session, request.getConsoleId());
            session.nonExport()
                    .requiresSerialQueue()
                    .require(exportedConsole)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        ScriptSession scriptSession = exportedConsole.get();

                        //produce a diff
                        ExecuteCommandResponse.Builder diff = ExecuteCommandResponse.newBuilder();

                        ScriptSession.Changes changes = scriptSession.evaluateScript(request.getCode());

                        changes.created.entrySet().forEach(entry -> diff.addCreated(makeVariableDefinition(entry)));
                        changes.updated.entrySet().forEach(entry -> diff.addUpdated(makeVariableDefinition(entry)));
                        changes.removed.entrySet().forEach(entry -> diff.addRemoved(makeVariableDefinition(entry)));

                        responseObserver.onNext(diff.build());
                        responseObserver.onCompleted();
                    });
        });
    }

    private static VariableDefinition makeVariableDefinition(Map.Entry<String, ExportedObjectType> entry) {
        return VariableDefinition.newBuilder().setName(entry.getKey()).setType(entry.getValue().name()).build();
    }

    @Override
    public void cancelCommand(CancelCommandRequest request, StreamObserver<CancelCommandResponse> responseObserver) {
        // TODO not yet implemented, need a way to handle stopping a command in a consistent way
        super.cancelCommand(request, responseObserver);
    }

    @Override
    public void bindTableToVariable(BindTableToVariableRequest request, StreamObserver<BindTableToVariableResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final SessionState.ExportObject<Table> exportedTable = ticketRouter.resolve(session, request.getTableId());
            final SessionState.ExportObject<ScriptSession> exportedConsole;

            ExportBuilder<?> exportBuilder = session.nonExport()
                    .requiresSerialQueue()
                    .onError(responseObserver::onError);

            if (request.hasConsoleId()) {
                exportedConsole = ticketRouter.resolve(session, request.getConsoleId());
                exportBuilder.require(exportedTable, exportedConsole);
            } else {
                exportedConsole = null;
                exportBuilder.require(exportedTable);
            }

            exportBuilder.submit(() -> {
                ScriptSession scriptSession = exportedConsole != null ? exportedConsole.get() : globalSessionProvider.getGlobalSession();
                Table table = exportedTable.get();
                scriptSession.setVariable(request.getVariableName(), table);
                scriptSession.manage(table);
                responseObserver.onNext(BindTableToVariableResponse.getDefaultInstance());
                responseObserver.onCompleted();
            });
        });
    }

    // TODO will be moved to a more general place, serve as a general "Fetch from scope" and this will be deprecated
    @Override
    public void fetchTable(FetchTableRequest request, StreamObserver<ExportedTableCreationResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<ScriptSession> exportedConsole = ticketRouter.resolve(session, request.getConsoleId());

            session.newExport(request.getTableId())
                    .require(exportedConsole)
                    .onError(responseObserver::onError)
                    .submit(() -> liveTableMonitor.exclusiveLock().computeLocked(() -> {
                        ScriptSession scriptSession = exportedConsole.get();
                        String tableName = request.getTableName();
                        if (!scriptSession.hasVariableName(tableName)) {
                            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND, "No value exists with name " + tableName);
                        }

                        // Explicit typecheck to catch any wrong-type-ness right away
                        Object result = scriptSession.unwrapObject(scriptSession.getVariable(tableName));
                        if (!(result instanceof Table)) {
                            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Value bound to name " + tableName + " is not a Table");
                        }

                        // Apply preview columns TODO core#107 move to table service
                        Table table = ColumnPreviewManager.applyPreview((Table) result);

                        safelyExecute(() -> {
                            final TableReference resultRef = TableReference.newBuilder().setTicket(request.getTableId()).build();
                            responseObserver.onNext(TableServiceGrpcImpl.buildTableCreationResponse(resultRef, table));
                            responseObserver.onCompleted();
                        });
                        return table;
                    }));
        });
    }

    private CompletionParser ensureParserForSession(SessionState session) {
        return parsers.computeIfAbsent(session, s -> {
            CompletionParser parser = new CompletionParser();
            s.addOnCloseCallback(parser);
            return parser;
        });
    }
    @Override
    public StreamObserver<AutoCompleteRequest> autoCompleteStream(StreamObserver<AutoCompleteResponse> responseObserver) {
        return GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            return new AutocompleteMessageMarshaller(responseObserver, session, ensureParserForSession(session));
        });
    }

//    @Override
//    public void openAutoCompleteStream(AutoCompleteRequest request, StreamObserver<AutoCompleteResponse> responseObserver) {
//        StreamData streamData = BrowserStreamInterceptor.STREAM_DATA_KEY.get();
//        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
//            final SessionState session = sessionService.getCurrentSession();
//
//            if (streamData == null) {
//                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no x-deephaven-stream headers, cannot handle open request");
//            }
//
//            AutocompleteMessageMarshaller autocomplete = new AutocompleteMessageMarshaller(responseObserver, session, ensureParserForSession(session));
//            BrowserStream<AutoCompleteRequest> browserStream = new BrowserStream<>(BrowserStream.Mode.IN_ORDER, session, autocomplete);
//            browserStream.onMessageReceived(request, streamData);
//
//            if (!streamData.isHalfClose()) {
//                // if this isn't a half-close, we should export it for later calls - if it is, the client won't send more messages
//                session.newExport(streamData.getRpcTicket())
//                        .onError(responseObserver::onError)
//                        .submit(() -> browserStream);
//            }
//        });
//    }
//
//    @Override
//    public void nextAutoCompleteStream(AutoCompleteRequest request, StreamObserver<BrowserNextResponse> responseObserver) {
//        StreamData streamData = BrowserStreamInterceptor.STREAM_DATA_KEY.get();
//        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
//            final SessionState session = sessionService.getCurrentSession();
//
//            if (streamData == null) {
//                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no x-deephaven-stream headers, cannot handle open request");
//            }
//
//            final SessionState.ExportObject<BrowserStream<AutoCompleteRequest>> browserStream =
//                    session.getExport(streamData.getRpcTicket());
//
//            session.nonExport()
//                    .require(browserStream)
//                    .onError(responseObserver::onError)
//                    .submit(() -> {
//                        browserStream.get().onMessageReceived(request, streamData);
//                        responseObserver.onNext(BrowserNextResponse.getDefaultInstance());
//                        responseObserver.onCompleted();
//                    });
//        });
//    }

    @Override
    public void fetchFigure(FetchFigureRequest request, StreamObserver<FetchFigureResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            SessionState.ExportObject<ScriptSession> exportedConsole = session.getExport(request.getConsoleId());

            session.nonExport()
                    .require(exportedConsole)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        ScriptSession scriptSession = exportedConsole.get();

                        String figureName = request.getFigureName();
                        if (!scriptSession.hasVariableName(figureName)) {
                            throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND, "No value exists with name " + figureName);
                        }

                        Object result = scriptSession.unwrapObject(scriptSession.getVariable(figureName));
                        if (!(result instanceof FigureWidget)) {
                            throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Value bound to name " + figureName + " is not a FigureWidget");
                        }
                        FigureWidget widget = (FigureWidget) result;

                        FigureDescriptor translated = FigureWidgetTranslator.translate(widget, session);

                        responseObserver.onNext(FetchFigureResponse.newBuilder().setFigureDescriptor(translated).build());
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

        private void tryClose () {
            if (session.removeOnCloseCallback(this) != null) {
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
            //      since LogBuffer is synchronized on access to the listeners. We're on the same thread
            //      as all other log receivers and
            try {
                LogSubscriptionData payload = LogSubscriptionData.newBuilder()
                        .setMicros(record.getTimestampMicros())
                        .setLogLevel(record.getLevel().getName())
                        //this could be done on either side, doing it here because its a weird charset and we should own that
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
