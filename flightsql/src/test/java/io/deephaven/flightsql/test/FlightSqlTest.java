//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.flightsql.test;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.auth.AuthContext;
import io.deephaven.base.clock.Clock;
import io.deephaven.client.impl.*;
import io.deephaven.csv.CsvTools;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.plugin.Registration;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ConfigServiceModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.plugin.PluginsModule;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.server.runner.MainHelper;
import io.deephaven.server.session.*;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.test.TestAuthModule;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.CallOptions;
import io.grpc.*;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.isNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class FlightSqlTest {
    @Module(includes = {
            ArrowModule.class,
            ConfigServiceModule.class,
            ConsoleModule.class,
            LogModule.class,
            SessionModule.class,
            TableModule.class,
            TestAuthModule.class,
            ObfuscatingErrorTransformerModule.class,
            PluginsModule.class,
    })
    public static class FlightTestModule {
        @IntoSet
        @Provides
        TicketResolver ticketResolver(ExportTicketResolver resolver) {
            return resolver;
        }

        @Singleton
        @Provides
        AbstractScriptSession<?> provideAbstractScriptSession(
                final UpdateGraph updateGraph,
                final OperationInitializer operationInitializer) {
            return new NoLanguageDeephavenSession(
                    updateGraph, operationInitializer, "non-script-session");
        }

        @Provides
        ScriptSession provideScriptSession(AbstractScriptSession<?> scriptSession) {
            return scriptSession;
        }

        @Provides
        Scheduler provideScheduler() {
            return new Scheduler.DelegatingImpl(
                    Executors.newSingleThreadExecutor(),
                    Executors.newScheduledThreadPool(1),
                    Clock.system());
        }

        @Provides
        @Named("session.tokenExpireMs")
        long provideTokenExpireMs() {
            return 60_000_000;
        }

        @Provides
        @Named("http.port")
        int provideHttpPort() {
            return 0;// 'select first available'
        }

        @Provides
        @Named("grpc.maxInboundMessageSize")
        int provideMaxInboundMessageSize() {
            return 1024 * 1024;
        }

        @Provides
        @Nullable
        ScheduledExecutorService provideExecutorService() {
            return null;
        }

        @Provides
        AuthorizationProvider provideAuthorizationProvider(TestAuthorizationProvider provider) {
            return provider;
        }

        @Provides
        @Singleton
        TestAuthorizationProvider provideTestAuthorizationProvider() {
            return new TestAuthorizationProvider();
        }

        @Provides
        @Singleton
        static UpdateGraph provideUpdateGraph() {
            return ExecutionContext.getContext().getUpdateGraph();
        }

        @Provides
        @Singleton
        static OperationInitializer provideOperationInitializer() {
            return ExecutionContext.getContext().getOperationInitializer();
        }
    }

    public interface TestComponent {
        Set<ServerInterceptor> interceptors();

        SessionServiceGrpcImpl sessionGrpcService();

        SessionService sessionService();

        GrpcServer server();

        TestAuthModule.BasicAuthTestImpl basicAuthHandler();

        ExecutionContext executionContext();

        TestAuthorizationProvider authorizationProvider();

        Registration.Callback registration();
    }

    private LogBuffer logBuffer;
    private GrpcServer server;
    protected int localPort;
    // private FlightClient flightClient;

    protected SessionService sessionService;

    private SessionState currentSession;
    private SafeCloseable executionContext;
    private Location serverLocation;
    protected TestComponent component;

    private ManagedChannel clientChannel;
    private ScheduledExecutorService clientScheduler;
    private Session clientSession;

    @BeforeAll
    public static void setupOnce() throws IOException {
        MainHelper.bootstrapProjectDirectories();
    }

    @BeforeEach
    public void setup() throws Exception {
        logBuffer = new LogBuffer(128);
        LogBufferGlobal.setInstance(logBuffer);

        component = component();
        // open execution context immediately so it can be used when resolving `scriptSession`
        executionContext = component.executionContext().open();

        server = component.server();
        server.start();
        localPort = server.getPort();

        sessionService = component.sessionService();

        serverLocation = Location.forGrpcInsecure("localhost", localPort);
        currentSession = sessionService.newSession(new AuthContext.SuperUser());

        clientChannel = ManagedChannelBuilder.forTarget("localhost:" + localPort)
                .usePlaintext()
                .intercept(new TestAuthClientInterceptor(currentSession.getExpiration().token.toString()))
                .build();

        clientScheduler = Executors.newSingleThreadScheduledExecutor();

        clientSession = SessionImpl
                .create(SessionImplConfig.from(SessionConfig.builder().build(), clientChannel, clientScheduler));

        setUpFlightSqlClient();

        final Table table = CsvTools.readCsv(
                "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/FakeCryptoTrades_20230209.csv");
        ExecutionContext.getContext().getQueryScope().putParam("crypto", table);
    }

    private static final class TestAuthClientInterceptor implements ClientInterceptor {
        final BearerHandler callCredentials = new BearerHandler();

        public TestAuthClientInterceptor(String bearerToken) {
            callCredentials.setBearerToken(bearerToken);
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions, Channel next) {
            return next.newCall(method, callOptions.withCallCredentials(callCredentials));
        }
    }

    protected abstract TestComponent component();

    @AfterEach
    public void teardown() throws InterruptedException {
        clientSession.close();
        clientScheduler.shutdownNow();
        clientChannel.shutdownNow();

        sessionService.closeAllSessions();
        executionContext.close();

        closeClient();
        server.stopWithTimeout(1, TimeUnit.MINUTES);

        try {
            server.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            server = null;
        }

        LogBufferGlobal.clear(logBuffer);
    }

    private void closeClient() {
        try {
            flightSqlClient.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static final String LOCALHOST = "localhost";
    protected static BufferAllocator allocator;
    protected static FlightSqlClient flightSqlClient;

    private void setUpFlightSqlClient() {
        allocator = new RootAllocator(Integer.MAX_VALUE);

        final Location clientLocation = Location.forGrpcInsecure(LOCALHOST, localPort);
        var middleware = new FlightClientMiddleware() {
            private String token;

            @Override
            public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                if (token != null) {
                    outgoingHeaders.insert("authorization", token);
                } else {
                    outgoingHeaders.insert("authorization", "Anonymous");
                }
            }

            @Override
            public void onHeadersReceived(CallHeaders incomingHeaders) {
                token = incomingHeaders.get("authorization");
            }

            @Override
            public void onCallCompleted(CallStatus status) {}
        };
        FlightClient flightClient = FlightClient.builder().location(clientLocation)
                .allocator(allocator).intercept(info -> middleware).build();
        // sqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
        flightSqlClient = new FlightSqlClient(flightClient);

    }

    @Test
    public void testCreateStatementResults() throws Exception {
        try (final FlightStream stream =
                flightSqlClient.getStream(
                        flightSqlClient.execute(
                                "SELECT * FROM crypto where Instrument='BTC/USD' AND Price > 50000 and Exchange = 'binance'")
                                .getEndpoints().get(0).getTicket())) {
            Schema schema = stream.getSchema();
            assertTrue(schema.getFields().size() == 5);
            List<List<String>> results = FlightSqlTest.getResults(stream);
            assertTrue(results.size() > 0);
        }
    }

    @Test
    public void testCreateStatementGroupByResults() throws Exception {
        try (final FlightStream stream =
                flightSqlClient.getStream(
                        flightSqlClient.execute("SELECT Exchange, Instrument, AVG(Price) " +
                                "FROM crypto where Instrument='BTC/USD' " +
                                "GROUP BY Exchange, Instrument")
                                .getEndpoints().get(0).getTicket())) {
            Schema schema = stream.getSchema();
            assertTrue(schema.getFields().size() == 3);
            List<List<String>> results = FlightSqlTest.getResults(stream);
            assertTrue(results.size() > 0);
        }
    }

    @Test
    public void testCreateStatementErrors() {
        {
            Exception exception = assertThrows(FlightRuntimeException.class, () -> {
                try (final FlightStream stream =
                        flightSqlClient.getStream(
                                flightSqlClient.execute("SELECT Exchange, Instrument, AVG(Price) " +
                                        "FROM crypto where Instrument='BTC/USD' " +
                                        "GROUP BY Exchange")
                                        .getEndpoints().get(0).getTicket())) {
                }
            });
            String expectedMessage = "calcite.runtime.CalciteContextException";
            assertTrue(exception.getMessage().contains(expectedMessage));
        }
        {
            Exception exception = assertThrows(FlightRuntimeException.class, () -> {
                try (final FlightStream stream =
                        flightSqlClient.getStream(
                                flightSqlClient.execute("SELECT Exchange, Instrument AVG(Price) " +
                                        "FROM crypto where Instrument='BTC/USD' " +
                                        "GROUP BY Exchange")
                                        .getEndpoints().get(0).getTicket())) {
                }
            });
            String expectedMessage = "SqlParseException";
            assertTrue(exception.getMessage().contains(expectedMessage));
        }
    }

    public static List<List<String>> getResults(FlightStream stream) {
        final List<List<String>> results = new ArrayList<>();
        while (stream.next()) {
            try (final VectorSchemaRoot root = stream.getRoot()) {
                final long rowCount = root.getRowCount();
                for (int i = 0; i < rowCount; ++i) {
                    results.add(new ArrayList<>());
                }

                root.getSchema()
                        .getFields()
                        .forEach(
                                field -> {
                                    try (final FieldVector fieldVector = root.getVector(field.getName())) {
                                        if (fieldVector instanceof VarCharVector) {
                                            final VarCharVector varcharVector = (VarCharVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Text data = varcharVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : data.toString());
                                            }
                                        } else if (fieldVector instanceof IntVector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof VarBinaryVector) {
                                            final VarBinaryVector varbinaryVector = (VarBinaryVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final byte[] data = varbinaryVector.getObject(rowIndex);
                                                final String output;
                                                try {
                                                    output =
                                                            isNull(data)
                                                                    ? null
                                                                    : MessageSerializer.deserializeSchema(
                                                                            new ReadChannel(
                                                                                    Channels.newChannel(
                                                                                            new ByteArrayInputStream(
                                                                                                    data))))
                                                                            .toJson();
                                                } catch (final IOException e) {
                                                    throw new RuntimeException("Failed to deserialize schema", e);
                                                }
                                                results.get(rowIndex).add(output);
                                            }
                                        } else if (fieldVector instanceof DenseUnionVector) {
                                            final DenseUnionVector denseUnionVector = (DenseUnionVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Object data = denseUnionVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof ListVector) {
                                            for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                                if (!fieldVector.isNull(i)) {
                                                    List<Text> elements =
                                                            (List<Text>) ((ListVector) fieldVector).getObject(i);
                                                    List<String> values = new ArrayList<>();

                                                    for (Text element : elements) {
                                                        values.add(element.toString());
                                                    }
                                                    results.get(i).add(values.toString());
                                                }
                                            }

                                        } else if (fieldVector instanceof UInt4Vector) {
                                            final UInt4Vector uInt4Vector = (UInt4Vector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Object data = uInt4Vector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof UInt1Vector) {
                                            final UInt1Vector uInt1Vector = (UInt1Vector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                final Object data = uInt1Vector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof BitVector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof TimeStampNanoTZVector) {
                                            TimeStampNanoTZVector timeStampNanoTZVector =
                                                    (TimeStampNanoTZVector) fieldVector;
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Long data = timeStampNanoTZVector.getObject(rowIndex);
                                                Instant instant = Instant.ofEpochSecond(0, data);
                                                results.get(rowIndex).add(isNull(instant) ? null : instant.toString());
                                            }
                                        } else if (fieldVector instanceof Float8Vector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof Float4Vector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else if (fieldVector instanceof DecimalVector) {
                                            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                                                Object data = fieldVector.getObject(rowIndex);
                                                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
                                            }
                                        } else {
                                            System.out.println("Unsupported vector type: " + fieldVector.getClass());
                                        }
                                    }
                                });
            }
        }
        return results;
    }
}

