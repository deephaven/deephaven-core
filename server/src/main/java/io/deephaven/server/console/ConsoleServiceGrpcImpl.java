//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.console;

import com.google.common.base.Throwables;
import com.google.rpc.Code;
import io.deephaven.base.LockFreeArrayQueue;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.RuntimeMemory;
import io.deephaven.engine.table.impl.util.RuntimeMemory.Sample;
import io.deephaven.engine.util.DelegatingScriptSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.integrations.python.PythonDeephavenSession;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferRecord;
import io.deephaven.io.logger.LogBufferRecordListener;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.completion.CustomCompletion;
import io.deephaven.proto.backplane.grpc.FieldInfo;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.proto.backplane.script.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.console.completer.JavaAutoCompleteObserver;
import io.deephaven.server.console.completer.PythonAutoCompleteObserver;
import io.deephaven.server.session.SessionCloseableObserver;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.SessionState.ExportBuilder;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.deephaven.extensions.barrage.util.GrpcUtil.*;

@Singleton
public class ConsoleServiceGrpcImpl extends ConsoleServiceGrpc.ConsoleServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ConsoleServiceGrpcImpl.class);

    public static final boolean REMOTE_CONSOLE_DISABLED =
            Configuration.getInstance().getBooleanWithDefault("deephaven.console.disable", false);

    private static final String DISABLE_AUTOCOMPLETE_FLAG = "deephaven.console.autocomplete.disable";
    public static final boolean AUTOCOMPLETE_DISABLED =
            Configuration.getInstance().getBooleanWithDefault(DISABLE_AUTOCOMPLETE_FLAG, false);


    public static final boolean QUIET_AUTOCOMPLETE_ERRORS =
            Configuration.getInstance().getBooleanWithDefault("deephaven.console.autocomplete.quiet", true);

    public static final long SUBSCRIBE_TO_LOGS_SEND_MILLIS =
            Configuration.getInstance().getLongWithDefault("deephaven.console.subscribeToLogs.sendMillis", 100);

    public static final String SUBSCRIBE_TO_LOGS_BUFFER_SIZE_PROP = "deephaven.console.subscribeToLogs.bufferSize";

    public static final int SUBSCRIBE_TO_LOGS_BUFFER_SIZE =
            Configuration.getInstance().getIntegerWithDefault(SUBSCRIBE_TO_LOGS_BUFFER_SIZE_PROP, 32768);

    private static final AtomicBoolean ALREADY_WARNED_ABOUT_NO_AUTOCOMPLETE = new AtomicBoolean();

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final Provider<ScriptSession> scriptSessionProvider;
    private final Scheduler scheduler;
    private final LogBuffer logBuffer;
    private final Set<CustomCompletion.Factory> customCompletionFactory;

    @Inject
    public ConsoleServiceGrpcImpl(final TicketRouter ticketRouter,
            final SessionService sessionService,
            final Provider<ScriptSession> scriptSessionProvider,
            final Scheduler scheduler,
            final LogBuffer logBuffer, Set<CustomCompletion.Factory> customCompletionFactory) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.scriptSessionProvider = scriptSessionProvider;
        this.scheduler = Objects.requireNonNull(scheduler);
        this.logBuffer = Objects.requireNonNull(logBuffer);
        this.customCompletionFactory = customCompletionFactory;
    }

    @Override
    public void getConsoleTypes(
            @NotNull final GetConsoleTypesRequest request,
            @NotNull final StreamObserver<GetConsoleTypesResponse> responseObserver) {
        // TODO (deephaven-core#3147): for legacy reasons, this method is used prior to authentication
        if (!REMOTE_CONSOLE_DISABLED) {
            responseObserver.onNext(GetConsoleTypesResponse.newBuilder()
                    .addConsoleTypes(scriptSessionProvider.get().scriptType().toLowerCase())
                    .build());
        } else {
            responseObserver.onNext(GetConsoleTypesResponse.getDefaultInstance());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void startConsole(
            @NotNull final StartConsoleRequest request,
            @NotNull final StreamObserver<StartConsoleResponse> responseObserver) {
        SessionState session = sessionService.getCurrentSession();
        if (REMOTE_CONSOLE_DISABLED) {
            responseObserver
                    .onError(Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Remote console disabled"));
            return;
        }

        // TODO (#702): initially global session will be null; set it here if applicable

        final String sessionType = request.getSessionType();
        if (!scriptSessionProvider.get().scriptType().equalsIgnoreCase(sessionType)) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "session type '" + sessionType + "' is not supported");
        }

        session.newExport(request.getResultId(), "resultId")
                .onError(responseObserver)
                .submit(() -> {
                    final ScriptSession scriptSession = new DelegatingScriptSession(scriptSessionProvider.get());
                    safelyOnNextAndComplete(responseObserver, StartConsoleResponse.newBuilder()
                            .setResultId(request.getResultId())
                            .build());
                    return scriptSession;
                });
    }

    @Override
    public void subscribeToLogs(
            @NotNull final LogSubscriptionRequest request,
            @NotNull final StreamObserver<LogSubscriptionData> responseObserver) {
        sessionService.getCurrentSession();
        if (REMOTE_CONSOLE_DISABLED) {
            safelyError(responseObserver, Code.FAILED_PRECONDITION, "Remote console disabled");
            return;
        }
        // Session close logic implicitly handled in
        // io.deephaven.server.session.SessionServiceGrpcImpl.SessionServiceInterceptor
        final LogsClient client =
                new LogsClient(request, (ServerCallStreamObserver<LogSubscriptionData>) responseObserver);
        client.start();
    }

    @Override
    public void executeCommand(
            @NotNull final ExecuteCommandRequest request,
            @NotNull final StreamObserver<ExecuteCommandResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();
        final Ticket consoleId = request.getConsoleId();
        if (consoleId.getTicket().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "No consoleId supplied");
        }

        final String description = "ConsoleService#executeCommand(console="
                + ticketRouter.getLogNameFor(consoleId, "consoleId") + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<ScriptSession> exportedConsole =
                    ticketRouter.resolve(session, consoleId, "consoleId");

            session.<ExecuteCommandResponse>nonExport()
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .requiresSerialQueue()
                    .require(exportedConsole)
                    .onError(responseObserver)
                    .onSuccess((final ExecuteCommandResponse response) -> safelyOnNextAndComplete(responseObserver,
                            response))
                    .submit(() -> {
                        final ScriptSession scriptSession = exportedConsole.get();
                        final ScriptSession.Changes changes = scriptSession.evaluateScript(request.getCode());
                        final ExecuteCommandResponse.Builder diff = ExecuteCommandResponse.newBuilder();
                        final FieldsChangeUpdate.Builder fieldChanges = FieldsChangeUpdate.newBuilder();
                        changes.created.entrySet()
                                .forEach(entry -> fieldChanges.addCreated(makeVariableDefinition(entry)));
                        changes.updated.entrySet()
                                .forEach(entry -> fieldChanges.addUpdated(makeVariableDefinition(entry)));
                        changes.removed.entrySet()
                                .forEach(entry -> fieldChanges.addRemoved(makeVariableDefinition(entry)));
                        if (changes.error != null) {
                            diff.setErrorMessage(Throwables.getStackTraceAsString(changes.error));
                            log.error().append("Error running script: ").append(changes.error).endl();
                        }
                        return diff.setChanges(fieldChanges).build();
                    });
        }
    }

    @Override
    public void getHeapInfo(
            @NotNull final GetHeapInfoRequest request,
            @NotNull final StreamObserver<GetHeapInfoResponse> responseObserver) {
        sessionService.getCurrentSession();
        final RuntimeMemory runtimeMemory = RuntimeMemory.getInstance();
        final Sample sample = new Sample();
        runtimeMemory.read(sample);
        final GetHeapInfoResponse infoResponse = GetHeapInfoResponse.newBuilder()
                .setTotalMemory(sample.totalMemory)
                .setFreeMemory(sample.freeMemory)
                .setMaxMemory(runtimeMemory.maxMemory())
                .build();
        responseObserver.onNext(infoResponse);
        responseObserver.onCompleted();
    }

    private static FieldInfo makeVariableDefinition(Map.Entry<String, String> entry) {
        return makeVariableDefinition(entry.getKey(), entry.getValue());
    }

    private static FieldInfo makeVariableDefinition(String title, String type) {
        final TypedTicket id = TypedTicket.newBuilder()
                .setType(type)
                .setTicket(ScopeTicketResolver.ticketForName(title))
                .build();
        return FieldInfo.newBuilder()
                .setApplicationId("scope")
                .setFieldName(title)
                .setFieldDescription("query scope variable")
                .setTypedTicket(id)
                .build();
    }

    @Override
    public void cancelCommand(
            @NotNull final CancelCommandRequest request,
            @NotNull final StreamObserver<CancelCommandResponse> responseObserver) {
        // TODO (#53): consider task cancellation
        super.cancelCommand(request, responseObserver);
    }

    @Override
    public void bindTableToVariable(
            @NotNull final BindTableToVariableRequest request,
            @NotNull final StreamObserver<BindTableToVariableResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();

        final Ticket tableId = request.getTableId();
        if (tableId.getTicket().isEmpty()) {
            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "No source tableId supplied");
        }

        final String description = "ConsoleService#bindTableToVariable(table="
                + ticketRouter.getLogNameFor(tableId, "tableId") + ", variableName=" + request.getVariableName()
                + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Table> exportedTable =
                    ticketRouter.resolve(session, tableId, "tableId");

            final SessionState.ExportObject<ScriptSession> exportedConsole;

            ExportBuilder<?> exportBuilder = session.nonExport()
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .requiresSerialQueue()
                    .onError(responseObserver)
                    .onSuccess(() -> safelyOnNextAndComplete(responseObserver,
                            BindTableToVariableResponse.getDefaultInstance()));

            if (request.hasConsoleId()) {
                exportedConsole = ticketRouter.resolve(session, request.getConsoleId(), "consoleId");
                exportBuilder.require(exportedTable, exportedConsole);
            } else {
                exportedConsole = null;
                exportBuilder.require(exportedTable);
            }

            exportBuilder.submit(() -> {
                QueryScope queryScope = exportedConsole != null ? exportedConsole.get().getQueryScope()
                        : ExecutionContext.getContext().getQueryScope();

                Table table = exportedTable.get();
                queryScope.putParam(request.getVariableName(), table);
            });
        }
    }

    @Override
    public StreamObserver<AutoCompleteRequest> autoCompleteStream(
            @NotNull final StreamObserver<AutoCompleteResponse> responseObserver) {
        final SessionState session = sessionService.getCurrentSession();
        if (AUTOCOMPLETE_DISABLED || ALREADY_WARNED_ABOUT_NO_AUTOCOMPLETE.get()) {
            return new NoopAutoCompleteObserver(session, responseObserver);
        }
        if (PythonDeephavenSession.SCRIPT_TYPE.equals(scriptSessionProvider.get().scriptType())) {
            PyObject[] settings = new PyObject[1];
            try {
                final ScriptSession scriptSession = scriptSessionProvider.get();
                scriptSession.evaluateScript(
                        "from deephaven_internal.auto_completer import jedi_settings ; jedi_settings.set_scope(globals())");
                settings[0] = scriptSession.getQueryScope().readParamValue("jedi_settings");
            } catch (Exception err) {
                if (!ALREADY_WARNED_ABOUT_NO_AUTOCOMPLETE.getAndSet(true)) {
                    log.error().append("Autocomplete package not found; disabling autocomplete.").endl();
                    log.error().append("Do you need to install the autocomplete package?").endl();
                    log.error().append("    pip install deephaven-core[autocomplete]==<version>").endl();
                    log.error().append("Add the jvm flag '-D").append(DISABLE_AUTOCOMPLETE_FLAG)
                            .append("=true' to disable this message.").endl();
                }
            }
            boolean canJedi = settings[0] != null && settings[0].call("can_jedi").getBooleanValue();
            log.info().append(canJedi ? "Using jedi for python autocomplete"
                    : "No jedi dependency available in python environment; disabling autocomplete.").endl();
            return canJedi ? new PythonAutoCompleteObserver(responseObserver, scriptSessionProvider, session)
                    : new NoopAutoCompleteObserver(session, responseObserver);
        }

        return new JavaAutoCompleteObserver(session, responseObserver, customCompletionFactory);
    }

    private static class NoopAutoCompleteObserver extends SessionCloseableObserver<AutoCompleteResponse>
            implements StreamObserver<AutoCompleteRequest> {
        public NoopAutoCompleteObserver(SessionState session, StreamObserver<AutoCompleteResponse> responseObserver) {
            super(session, responseObserver);
        }

        @Override
        public void onNext(AutoCompleteRequest value) {
            // This implementation only responds to autocomplete requests with "success, nothing found"
            AutoCompleteResponse.Builder responseBuilder = AutoCompleteResponse.newBuilder()
                    .setSuccess(true)
                    .setRequestId(value.getRequestId());

            if (value.getRequestCase() == AutoCompleteRequest.RequestCase.GET_COMPLETION_ITEMS) {
                // For maintaining backwards compatibility
                responseBuilder.setCompletionItems(
                        GetCompletionItemsResponse.newBuilder()
                                .setSuccess(true)
                                .setRequestId(value.getRequestId()));
            }
            safelyOnNext(responseObserver, responseBuilder.build());
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

    @Override
    public void cancelAutoComplete(
            @NotNull final CancelAutoCompleteRequest request,
            @NotNull final StreamObserver<CancelAutoCompleteResponse> responseObserver) {
        // TODO: Consider autocomplete cancellation
        super.cancelAutoComplete(request, responseObserver);
    }

    private final class LogsClient implements LogBufferRecordListener, Runnable {
        private final LogSubscriptionRequest request;
        private final ServerCallStreamObserver<LogSubscriptionData> client;
        private final LockFreeArrayQueue<LogSubscriptionData> buffer;
        private final AtomicBoolean guard;
        private volatile boolean done;
        private volatile boolean tooSlow;

        public LogsClient(
                final LogSubscriptionRequest request,
                final ServerCallStreamObserver<LogSubscriptionData> client) {
            this.request = Objects.requireNonNull(request);
            this.client = Objects.requireNonNull(client);
            // Our buffer capacity should always be greater than the capacity of the logBuffer; otherwise, the initial
            // act of subscribeToLogs could fully fill up this buffer (immediately causing the tooSlow case).
            //
            // Ideally, the buffer should be sized based on the maximum expected logging rate and the behavior of the
            // clients subscribing to logs.
            this.buffer = LockFreeArrayQueue.of(Math.max(SUBSCRIBE_TO_LOGS_BUFFER_SIZE, logBuffer.capacity() * 2));
            this.guard = new AtomicBoolean(false);
            this.client.setOnReadyHandler(this::onReady);
            this.client.setOnCancelHandler(this::onCancel);
            this.client.setOnCloseHandler(this::onClose);
        }

        public void start() {
            logBuffer.subscribe(this);
            scheduler.runImmediately(this);
        }

        public void stop() {
            safelyComplete(client);
        }

        // ------------------------------------------------------------------------------------------------------------

        @Override
        public void record(LogBufferRecord record) {
            if (done) {
                return;
            }
            // only pass levels the client wants
            if (request.getLevelsCount() != 0 && !request.getLevelsList().contains(record.getLevel().getName())) {
                return;
            }
            // since the subscribe() method auto-replays all existing logs, filter to just once this consumer probably
            // hasn't seen
            if (record.getTimestampMicros() <= request.getLastSeenLogTimestamp()) {
                return;
            }
            // Note: we can't send record off-thread without doing a deepCopy.
            // io.deephaven.io.logger.LogBufferInterceptor owns io.deephaven.io.logger.LogBufferRecord.getData.
            // We can side-step this problem by creating the appropriate response here.
            final LogSubscriptionData payload = LogSubscriptionData.newBuilder()
                    .setMicros(record.getTimestampMicros())
                    .setLogLevel(record.getLevel().getName())
                    .setMessage(record.getDataString())
                    .build();
            enqueue(payload);
        }

        // ------------------------------------------------------------------------------------------------------------

        // Our implementation relies on backpressure from the gRPC network stack; and an application-level, per-stream,
        // buffer. Our application buffer gives us capacity for handling logging bursts that exceed the achievable
        // bandwidth between server and client. Ultimately, the achievable bandwidth is a function of the gRPC
        // networking stack. Unfortunately, there are not many knobs that gRPC java currently exposes for tuning these
        // parts of the stack.
        // See https://github.com/grpc/proposal/pull/135, A25: make backpressure threshold configurable
        // See https://github.com/grpc/grpc-java/issues/5433, Allow configuring onReady threshold

        @Override
        public void run() {
            while (!done) {
                if (!guard.compareAndSet(false, true)) {
                    return;
                }
                boolean bufferIsKnownEmpty;
                try {
                    bufferIsKnownEmpty = false;
                    while (true) {
                        if (done) {
                            return;
                        }
                        if (tooSlow) {
                            safelyError(client, Code.RESOURCE_EXHAUSTED, String.format(
                                    "Too slow: the client or network may be too slow to keep up with the logging rates, or there may be logging bursts that exceed the available buffer size. The buffer size can be configured through the server property '%s'.",
                                    SUBSCRIBE_TO_LOGS_BUFFER_SIZE_PROP));
                            return;
                        }
                        if (!client.isReady()) {
                            break;
                        }
                        final LogSubscriptionData payload = dequeue();
                        if (payload == null) {
                            bufferIsKnownEmpty = true;
                            break;
                        }
                        safelyOnNext(client, payload);
                    }
                } finally {
                    guard.set(false);
                }
                if (!client.isReady()) {
                    // When !client.isReady(), onReady() is going to be called and will handle the reschedule.
                    // Note: it's important that client.isReady() is checked _outside_ of the guard block (otherwise,
                    // the onReady() call could execute and schedule before the current thread exits the guard block,
                    // and we'd fail to successfully reschedule).
                    return;
                }
                if (bufferIsKnownEmpty) {
                    // TODO(deephaven-core#3396): Add io.deephaven.server.util.Scheduler fixed delay support
                    scheduler.runAfterDelay(SUBSCRIBE_TO_LOGS_SEND_MILLIS, this);
                    return;
                }
                // Continue sending until the client's buffer is full or our buffer is empty (or done, or tooSlow).
            }
        }

        // ------------------------------------------------------------------------------------------------------------

        private void onReady() {
            scheduler.runImmediately(this);
        }

        private void onClose() {
            done = true;
            logBuffer.unsubscribe(this);
        }

        private void onCancel() {
            done = true;
            logBuffer.unsubscribe(this);
        }

        // ------------------------------------------------------------------------------------------------------------
        // Buffer implementation
        // ------------------------------------------------------------------------------------------------------------
        // The implementation needs to support single-producer / single-consumer concurrency. The current implementation
        // uses a io.deephaven.base.LockFreeArrayQueue, and chooses to close clients who are too slow to keep up.
        //
        // If desired, the implementation could be changed so that slow clients are allowed to stay connected, but start
        // missing logging data (either, they miss the latest data [queue-based impl], or they miss older data
        // [ringbuffer-based impl]).

        private void enqueue(LogSubscriptionData payload) {
            if (!buffer.enqueue(payload)) {
                // Just like we don't want to do onNext(payload) on this thread, we'll have the scheduler handle the
                // tooSlow error.
                tooSlow = true;
                // Note: the unsubscribe here will happen on the LogBufferRecordListener#record() caller's thread
                logBuffer.unsubscribe(this);
                scheduler.runImmediately(this);
            }
        }

        private LogSubscriptionData dequeue() {
            return buffer.dequeue();
        }
    }
}
