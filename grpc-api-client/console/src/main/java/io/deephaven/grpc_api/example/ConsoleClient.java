/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.example;

import io.deephaven.grpc_api.console.ScopeTicketResolver;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.logger.Logger;
import com.google.protobuf.ByteString;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.grpc_api.runner.DeephavenApiServerModule;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.backplane.script.grpc.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Schema;

import java.io.Console;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class ConsoleClient {
    private static final Logger log = LoggerFactory.getLogger(ConsoleClient.class);
    private Ticket consoleTicket;

    public static class RemoteStdout {
        private static final Logger log = LoggerFactory.getLogger(RemoteStdout.class);

        private RemoteStdout() {}
    }

    public static class RemoteStderr {
        private static final Logger log = LoggerFactory.getLogger(RemoteStderr.class);

        private RemoteStderr() {}
    }

    public static void main(final String[] args) throws Exception {
        // Assign properties that need to be set to even turn on
        System.setProperty("Configuration.rootFile", "grpc-api.prop");
        System.setProperty("io.deephaven.configuration.PropertyInputStreamLoader.override",
                "io.deephaven.configuration.PropertyInputStreamLoaderTraditional");

        final String sessionType = System.getProperty("console.sessionType", "groovy");
        log.info().append("Session type ").append(sessionType).endl();

//        final String target = args.length == 0 ? "localhost:8080" : args[0];
        final String target = args.length == 0 ? "localhost:8888" : args[0];
        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        final Scheduler scheduler = DeephavenApiServerModule.provideScheduler(2);
        final ConsoleClient client = new ConsoleClient(scheduler, channel, sessionType);
        Runtime.getRuntime().addShutdownHook(new Thread(client::stop));

        client.start();
        client.blockUntilShutdown();
    }

    private final Scheduler scheduler;
    private final Channel serverChannel;
    private final CountDownLatch shutdownRequested = new CountDownLatch(1);

    private final SessionServiceGrpc.SessionServiceStub sessionService;
    private final ConsoleServiceGrpc.ConsoleServiceStub consoleServiceGrpc;
    private final TableServiceGrpc.TableServiceStub tableServiceGrpc;
    private final String sessionType;

    private UUID session;
    private String sessionHeader;
    private Metadata.Key<String> sessionHeaderKey;

    private ConsoleClient(final Scheduler scheduler, final ManagedChannel managedChannel, String sessionType) {
        this.scheduler = scheduler;
        this.serverChannel = ClientInterceptors.intercept(managedChannel, new AuthInterceptor());
        this.sessionService = SessionServiceGrpc.newStub(serverChannel);
        this.consoleServiceGrpc = ConsoleServiceGrpc.newStub(serverChannel);
        this.tableServiceGrpc = TableServiceGrpc.newStub(serverChannel);
        this.sessionType = sessionType;
    }

    private void start() {
        LiveTableMonitor.DEFAULT.start();

        // no payload in this simple server auth
        sessionService.newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build(),
                new ResponseBuilder<HandshakeResponse>()
                        .onError(this::onError)
                        .onComplete(this::startConsole)
                        .onNext(this::onNewHandshakeResponse)
                        .build());
    }

    private void stop() {
        if (consoleTicket != null) {
            // clean up our console and its scope
            sessionService.release(
                    ReleaseRequest.newBuilder().setId(consoleTicket).build(),
                    new ResponseBuilder<ReleaseResponse>().build());
        }

        shutdownRequested.countDown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        shutdownRequested.await();
    }

    private int nextId = 1;

    private void startConsole() {
        consoleTicket = ExportTicketHelper.wrapExportIdInTicket(nextId++);
        consoleServiceGrpc.startConsole(StartConsoleRequest.newBuilder()
                .setResultId(consoleTicket)
                .setSessionType(sessionType)
                .build(),
                new ResponseBuilder<StartConsoleResponse>()
                        .onNext(response -> scheduler.runImmediately(this::awaitCommand))
                        .build());
        LogSubscriptionRequest request = LogSubscriptionRequest.newBuilder()
                .addLevels("STDOUT")
                .addLevels("STDERR")
                .setLastSeenLogTimestamp(System.currentTimeMillis() * 1000) // don't replay any logs that came before
                                                                            // now
                .build();
        consoleServiceGrpc.subscribeToLogs(request,
                new StreamObserver<LogSubscriptionData>() {
                    private String stripLastNewline(String msg) {
                        if (msg.endsWith("\n")) {
                            return msg.substring(0, msg.length() - 1);
                        }
                        return msg;
                    }

                    @Override
                    public void onNext(LogSubscriptionData value) {
                        if ("STDOUT".equals(value.getLogLevel())) {
                            RemoteStdout.log.info()
                                    .append(stripLastNewline(value.getMessage()))
                                    .endl();
                        } else if ("STDERR".equals(value.getLogLevel())) {
                            RemoteStderr.log.info()
                                    .append(stripLastNewline(value.getMessage()))
                                    .endl();
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        log.error(t).append("onError").endl();
                        stop();
                    }

                    @Override
                    public void onCompleted() {
                        // TODO reconnect
                    }
                });
    }

    private void awaitCommand() {
        Console console = System.console();
        if (console == null) {
            log.error().append("Can't open a console prompt, try running this outside of gradle?").endl();
            stop();
            return;
        }
        String userCode = console.readLine(String.format("%s> ", sessionType));
        if (userCode == null) {
            stop();
            return;
        }
        log.debug().append("client preparing to send command").endl();
        consoleServiceGrpc.executeCommand(
                ExecuteCommandRequest.newBuilder()
                        .setConsoleId(consoleTicket)
                        .setCode(userCode)
                        .build(),
                new ResponseBuilder<ExecuteCommandResponse>()
                        .onNext(response -> {
                            log.debug().append("command completed successfully: ").append(response.toString()).endl();
                            Optional<VariableDefinition> firstTable = response.getCreatedList().stream()
                                    .filter(var -> var.getType().equals("Table")).findAny();
                            firstTable.ifPresent(table -> {
                                log.debug().append("A table was created: ").append(table.toString()).endl();
                                tableServiceGrpc.fetchTable(FetchTableRequest.newBuilder()
                                        .setResultId(ExportTicketHelper.wrapExportIdInTicket(nextId++))
                                        .setSourceId(TableReference.newBuilder()
                                                .setTicket(ScopeTicketResolver.ticketForName(table.getTitle())))
                                        .build(),
                                        new ResponseBuilder<ExportedTableCreationResponse>()
                                                .onNext(this::onExportedTableCreationResponse)
                                                .onError(err -> {
                                                    log.error(err).append("onError").endl();
                                                    scheduler.runImmediately(this::awaitCommand);
                                                })
                                                .onComplete(() -> {
                                                    log.debug().append("fetch complete").endl();
                                                })
                                                .build());
                            });
                            // otherwise go for another query
                            if (!firstTable.isPresent()) {
                                // let's give the just-executed command a little bit of time to print so we reduce
                                // the chance of clobbering our stdin prompt.
                                scheduler.runAfterDelay(100, this::awaitCommand);
                            }
                        })
                        .onError(err -> {
                            log.error(err).append("onError").endl();
                            scheduler.runImmediately(this::awaitCommand);
                        })
                        .build());
    }

    private void onNewHandshakeResponse(final HandshakeResponse result) {
        session = UUID.fromString(result.getSessionToken().toStringUtf8());
        final String responseHeader = result.getMetadataHeader().toStringUtf8();
        if (sessionHeader == null || !sessionHeader.equals(responseHeader)) {
            sessionHeader = responseHeader;
            sessionHeaderKey = Metadata.Key.of(sessionHeader, Metadata.ASCII_STRING_MARSHALLER);
        }
        log.debug().append("Session Details: {header: '")
                .append(this.sessionHeader).append("', token: '")
                .append(this.session.toString()).append("}").endl();

        // Guess a good time to do the next refresh.
        final long refreshDelayMs = Math.min(
                scheduler.currentTime().getMillis() + result.getTokenExpirationDelayMillis() / 3,
                result.getTokenDeadlineTimeMillis() - result.getTokenExpirationDelayMillis() / 10);

        scheduler.runAtTime(DBTimeUtils.millisToTime(refreshDelayMs), this::refreshToken);
    }

    private void refreshToken() {
        sessionService.refreshSessionToken(HandshakeRequest.newBuilder()
                .setAuthProtocol(0)
                .setPayload(ByteString.copyFromUtf8(session.toString())).build(),
                new ResponseBuilder<HandshakeResponse>()
                        .onError(this::onError)
                        .onNext(this::onNewHandshakeResponse)
                        .build());
    }

    private void onExportedTableCreationResponse(final ExportedTableCreationResponse result) {
        final LogEntry entry = log.info().append("Received ExportedTableCreationResponse for {");

        if (result.getResultId().hasTicket()) {
            entry.append("exportId: ")
                    .append(ExportTicketHelper.ticketToExportId(result.getResultId().getTicket(), "resultId"));
        } else {
            entry.append("batchOffset: ").append(result.getResultId().getBatchOffset());
        }

        entry.append(", columns: {\n");
        Schema schema = Schema.getRootAsSchema(result.getSchemaHeader().asReadOnlyByteBuffer());
        for (int i = 0; i < schema.fieldsLength(); i++) {
            Field field = schema.fields(i);
            entry.append("\t");
            entry.append(field.name()).append(" : ").append(field.typeType()).append("\n");
            for (int j = 0; j < field.customMetadataLength(); j++) {
                KeyValue keyValue = field.customMetadata(j);
                entry.append("\t\t").append(keyValue.key()).append(" = ").append(keyValue.value());
                entry.append("\n");
            }
            entry.append("\n");
        }
        entry.append("}, attributes: {\n");
        for (int i = 0; i < schema.customMetadataLength(); i++) {
            entry.append("\t");
            KeyValue keyValue = schema.customMetadata(i);
            entry.append(keyValue.key()).append(" = ").append(keyValue.value());
            entry.append("\n");
        }
        entry.append("}\n");

        entry.append("}").endl();

        scheduler.runImmediately(this::awaitCommand);
    }

    private void onError(final Throwable t) {
        log.error().append("Unexpected error ").append(t).endl();
        stop();
    }

    private static class ResponseBuilder<T> {
        private Consumer<T> _onNext;
        private Consumer<Throwable> _onError;
        private Runnable _onComplete;

        public ResponseBuilder<T> onNext(final Consumer<T> _onNext) {
            this._onNext = _onNext;
            return this;
        }

        public ResponseBuilder<T> onError(final Consumer<Throwable> _onError) {
            this._onError = _onError;
            return this;
        }

        public ResponseBuilder<T> onComplete(final Runnable _onComplete) {
            this._onComplete = _onComplete;
            return this;
        }

        public StreamObserver<T> build() {
            return new StreamObserver<T>() {
                @Override
                public void onNext(final T value) {
                    if (_onNext != null)
                        _onNext.accept(value);
                }

                @Override
                public void onError(final Throwable t) {
                    if (_onError != null)
                        _onError.accept(t);
                }

                @Override
                public void onCompleted() {
                    if (_onComplete != null)
                        _onComplete.run();
                }
            };
        }
    }

    private class AuthInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                final Channel channel) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    channel.newCall(methodDescriptor, callOptions)) {
                @Override
                public void start(final Listener<RespT> responseListener, final Metadata headers) {
                    if (session != null) {
                        headers.put(sessionHeaderKey, session.toString());
                    }
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
