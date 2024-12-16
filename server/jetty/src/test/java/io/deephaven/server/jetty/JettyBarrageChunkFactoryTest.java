//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AuthContext;
import io.deephaven.base.clock.Clock;
import io.deephaven.client.impl.BearerHandler;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.plugin.Registration;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ConfigServiceModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.plugin.PluginsModule;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.server.runner.MainHelper;
import io.deephaven.server.session.ObfuscatingErrorTransformerModule;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionServiceGrpcImpl;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolver;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;
import io.deephaven.server.test.TestAuthModule;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.ServerInterceptor;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JettyBarrageChunkFactoryTest {
    private static final String COLUMN_NAME = "test_col";

    @Module
    public interface JettyTestConfig {
        @Provides
        static JettyConfig providesJettyConfig() {
            return JettyConfig.builder()
                    .port(0)
                    .tokenExpire(Duration.of(5, ChronoUnit.MINUTES))
                    .build();
        }
    }

    @Singleton
    @Component(modules = {
            ExecutionContextUnitTestModule.class,
            FlightMessageRoundTripTest.FlightTestModule.class,
            JettyServerModule.class,
            JettyFlightRoundTripTest.JettyTestConfig.class,
    })
    public interface JettyTestComponent extends FlightMessageRoundTripTest.TestComponent {
    }

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
        TicketResolver ticketResolver(ScopeTicketResolver resolver) {
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
    private int localPort;
    private FlightClient flightClient;
    private BufferAllocator allocator;

    protected SessionService sessionService;

    private SessionState currentSession;
    private SafeCloseable executionContext;
    private Location serverLocation;
    private FlightMessageRoundTripTest.TestComponent component;

    private ManagedChannel clientChannel;
    private ScheduledExecutorService clientScheduler;
    private Session clientSession;

    private int nextTicket = 1;

    @BeforeClass
    public static void setupOnce() throws IOException {
        MainHelper.bootstrapProjectDirectories();
    }

    @Before
    public void setup() throws IOException, InterruptedException {
        logBuffer = new LogBuffer(128);
        LogBufferGlobal.setInstance(logBuffer);

        component = DaggerJettyBarrageChunkFactoryTest_JettyTestComponent.create();
        // open execution context immediately so it can be used when resolving `scriptSession`
        executionContext = component.executionContext().open();

        server = component.server();
        server.start();
        localPort = server.getPort();

        sessionService = component.sessionService();

        serverLocation = Location.forGrpcInsecure("localhost", localPort);
        currentSession = sessionService.newSession(new AuthContext.SuperUser());
        allocator = new RootAllocator();
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(allocator).intercept(info -> new FlightClientMiddleware() {
                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        String token = currentSession.getExpiration().token.toString();
                        outgoingHeaders.insert("Authorization", Auth2Constants.BEARER_PREFIX + token);
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {}

                    @Override
                    public void onCallCompleted(CallStatus status) {}
                }).build();

        clientChannel = ManagedChannelBuilder.forTarget("localhost:" + localPort)
                .usePlaintext()
                .intercept(new TestAuthClientInterceptor(currentSession.getExpiration().token.toString()))
                .build();

        clientScheduler = Executors.newSingleThreadScheduledExecutor();

        clientSession = SessionImpl
                .create(SessionImplConfig.from(SessionConfig.builder().build(), clientChannel, clientScheduler));
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

    @After
    public void teardown() {
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
            flightClient.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Rule
    public final ExternalResource livenessRule = new ExternalResource() {
        SafeCloseable scope;

        @Override
        protected void before() {
            scope = LivenessScopeStack.open();
        }

        @Override
        protected void after() {
            if (scope != null) {
                scope.close();
                scope = null;
            }
        }
    };

    private void fullyReadStream(Ticket ticket, boolean expectError) {
        try (final FlightStream stream = flightClient.getStream(ticket)) {
            // noinspection StatementWithEmptyBody
            while (stream.next());
            if (expectError) {
                fail("expected error");
            }
        } catch (Exception ignored) {
        }
    }

    private Schema createSchema(final ArrowType arrowType, final Class<?> dhType) {
        return createSchema(arrowType, dhType, null);
    }

    private Schema createSchema(final ArrowType arrowType, final Class<?> dhType, final Class<?> dhComponentType) {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG, dhType.getCanonicalName());
        if (dhComponentType != null) {
            attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_COMPONENT_TYPE_TAG,
                    dhComponentType.getCanonicalName());
        }
        final FieldType fieldType = new FieldType(true, arrowType, null, attrs);
        return new Schema(Collections.singletonList(
                new Field(COLUMN_NAME, fieldType, null)));
    }

    @Test
    public void testInt8() throws Exception {
        final int numRows = 16;
        final Consumer<VectorSchemaRoot> setupData = source -> {
            IntVector vector = (IntVector) source.getFieldVectors().get(0);
            for (int ii = 0; ii < numRows; ++ii) {
                if (ii % 2 == 0) {
                    vector.setNull(ii);
                } else {
                    vector.set(ii, (byte) (ii - 8));
                }
            }
            source.setRowCount(numRows);
        };
        final BiConsumer<VectorSchemaRoot, VectorSchemaRoot> validator = (source, dest) -> {
            IntVector sVector = (IntVector) source.getVector(0);
            IntVector dVector = (IntVector) dest.getVector(0);
            for (int ii = 0; ii < numRows; ii++) {
                if (ii % 2 == 0) {
                    assertTrue(dVector.isNull(ii));
                } else {
                    assertEquals(sVector.get(ii), dVector.get(ii));
                }
            }
        };
        final Consumer<Class<?>> runForDhType = dhType -> {
            Schema schema = createSchema(Types.MinorType.INT.getType(), dhType);
            testRoundTrip(dhType, null, schema, setupData, validator);
        };

        runForDhType.accept(byte.class);
//        runForDhType.accept(char.class);
        runForDhType.accept(short.class);
        runForDhType.accept(int.class);
        runForDhType.accept(long.class);
        runForDhType.accept(float.class);
        runForDhType.accept(double.class);
        runForDhType.accept(BigInteger.class);
        runForDhType.accept(BigDecimal.class);
    }

    private void testRoundTrip(
            @NotNull final Class<?> dhType,
            @Nullable final Class<?> componentType,
            @NotNull final Schema schema,
            @NotNull final Consumer<VectorSchemaRoot> setupData,
            @NotNull final BiConsumer<VectorSchemaRoot, VectorSchemaRoot> validator) {
        try (VectorSchemaRoot source = VectorSchemaRoot.create(schema, allocator)) {
            source.allocateNew();
            setupData.accept(source);

            int flightDescriptorTicketValue = nextTicket++;
            FlightDescriptor descriptor = FlightDescriptor.path("export", flightDescriptorTicketValue + "");
            FlightClient.ClientStreamListener putStream = flightClient.startPut(descriptor, source, new AsyncPutListener());
            putStream.putNext();
            putStream.completed();

            // get the table that was uploaded, and confirm it matches what we originally sent
            CompletableFuture<Table> tableFuture = new CompletableFuture<>();
            SessionState.ExportObject<Table> tableExport = currentSession.getExport(flightDescriptorTicketValue);
            currentSession.nonExport()
                    .onErrorHandler(exception -> tableFuture.cancel(true))
                    .require(tableExport)
                    .submit(() -> tableFuture.complete(tableExport.get()));

            // block until we're done, so we can get the table and see what is inside
            putStream.getResult();
            Table uploadedTable = tableFuture.get();
            assertEquals(source.getRowCount(), uploadedTable.size());
            assertEquals(1, uploadedTable.getColumnSourceMap().size());
            ColumnSource<Object> columnSource = uploadedTable.getColumnSource(COLUMN_NAME);
            assertNotNull(columnSource);
            assertEquals(columnSource.getType(), dhType);
            assertEquals(columnSource.getComponentType(), componentType);

            try (FlightStream stream = flightClient.getStream(flightTicketFor(flightDescriptorTicketValue))) {
                VectorSchemaRoot dest = stream.getRoot();

                int numPayloads = 0;
                while (stream.next()) {
                    assertEquals(source.getRowCount(), dest.getRowCount());
                    validator.accept(source, dest);
                    ++numPayloads;
                }

                assertEquals(1, numPayloads);
            }
        } catch (Exception err) {
            throw new UncheckedDeephavenException("round trip test failure", err);
        }
    }

    private static Ticket flightTicketFor(int flightDescriptorTicketValue) {
        return new Ticket(FlightExportTicketHelper.exportIdToFlightTicket(flightDescriptorTicketValue).getTicket()
                .toByteArray());
    }
}
