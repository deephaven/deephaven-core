//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.auth.AuthContext;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
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
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.VectorFactory;
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
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JettyBarrageChunkFactoryTest {
    private static final String COLUMN_NAME = "test_col";
    private static final int NUM_ROWS = 1023;
    private static final int RANDOM_SEED = 42;
    private static final int MAX_LIST_ITEM_LEN = 3;

    private static final String DH_TYPE_TAG = BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG;
    private static final String DH_COMPONENT_TYPE_TAG =
            BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_COMPONENT_TYPE_TAG;

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

    private Schema createSchema(boolean nullable, ArrowType arrowType, Class<?> dhType) {
        return createSchema(nullable, arrowType, dhType, null);
    }

    private Schema createSchema(
            final boolean nullable,
            final ArrowType arrowType,
            final Class<?> dhType,
            final Class<?> dhComponentType) {
        final Map<String, String> attrs = new HashMap<>();
        attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG, dhType.getCanonicalName());
        if (dhComponentType != null) {
            attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_COMPONENT_TYPE_TAG,
                    dhComponentType.getCanonicalName());
        }
        final FieldType fieldType = new FieldType(nullable, arrowType, null, attrs);
        return new Schema(Collections.singletonList(
                new Field(COLUMN_NAME, fieldType, null)));
    }

    @Test
    public void testNumRowsIsOdd() {
        // ensure that rows are odd so that we hit padding lines
        assertEquals(NUM_ROWS % 2, 1);
    }

    @Test
    public void testInt8() throws Exception {
        class Test extends IntRoundTripTest<TinyIntVector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super(TinyIntVector::get, QueryConstants.NULL_BYTE, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(8, true), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final TinyIntVector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_BYTE, QueryConstants.MAX_BYTE, (byte) -1, (byte) 0, (byte) 1);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    byte value = (byte) rnd.nextInt();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_BYTE) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        new Test(byte.class, Number::byteValue, QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> (byte) (char) n.intValue(), (byte) QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, Number::shortValue, QueryConstants.NULL_SHORT).runTest();
        new Test(int.class).runTest();
        new Test(long.class).runTest();
        new Test(float.class).runTest();
        new Test(double.class).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testUint8() throws Exception {
        class Test extends IntRoundTripTest<UInt1Vector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super(UInt1Vector::get, QueryConstants.NULL_BYTE, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(8, false), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final UInt1Vector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_BYTE, QueryConstants.MAX_BYTE, (byte) -1, (byte) 0, (byte) 1);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    byte value = (byte) rnd.nextInt();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_BYTE) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        new Test(byte.class, Number::byteValue, QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> (byte) (char) n.intValue(), (byte) QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, Number::shortValue, QueryConstants.NULL_SHORT).runTest();
        new Test(int.class).runTest();
        new Test(long.class).runTest();
        new Test(float.class).runTest();
        new Test(double.class).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testInt16() throws Exception {
        class Test extends IntRoundTripTest<SmallIntVector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super(SmallIntVector::get, QueryConstants.NULL_SHORT, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(16, true), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final SmallIntVector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_SHORT, QueryConstants.MAX_SHORT, (short) -1, (short) 0, (short) 1);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    short value = (short) rnd.nextInt();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_SHORT) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        new Test(byte.class, Number::byteValue, QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> (short) (char) n.intValue(), (short) QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, Number::shortValue, QueryConstants.NULL_SHORT).runTest();
        new Test(int.class).runTest();
        new Test(long.class).runTest();
        new Test(float.class).runTest();
        new Test(double.class).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testUint16() throws Exception {
        class Test extends IntRoundTripTest<UInt2Vector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super((v, ii) -> (long) v.get(ii), QueryConstants.NULL_CHAR, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(16, false), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final UInt2Vector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_CHAR, QueryConstants.MAX_CHAR, (char) 1);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    char value = (char) rnd.nextInt();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_CHAR) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        // convert to char to avoid sign extension, then an int to return a Number
        new Test(byte.class, n -> (int) (char) n.byteValue(), (int) (char) QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> (int) (char) n.intValue(), (int) QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, n -> (int) (char) n.shortValue(), (int) (char) QueryConstants.NULL_SHORT).runTest();
        new Test(int.class).runTest();
        new Test(long.class).runTest();
        new Test(float.class).runTest();
        new Test(double.class).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testInt32() throws Exception {
        class Test extends IntRoundTripTest<IntVector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super(IntVector::get, QueryConstants.NULL_INT, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(32, true), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final IntVector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_INT, QueryConstants.MAX_INT, -1, 0, 1);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    int value = rnd.nextInt();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_INT) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        new Test(byte.class, Number::byteValue, QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> (int) (char) n.intValue(), (int) QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, Number::shortValue, QueryConstants.NULL_SHORT).runTest();
        new Test(int.class).runTest();
        new Test(long.class).runTest();
        new Test(float.class, n -> (int) n.floatValue(), (int) QueryConstants.NULL_FLOAT).runTest();
        new Test(double.class).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testUint32() throws Exception {
        class Test extends IntRoundTripTest<UInt4Vector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super(UInt4Vector::get, QueryConstants.NULL_INT, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(32, false), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final UInt4Vector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_INT, QueryConstants.MAX_INT, -1, 0, 1);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    int value = rnd.nextInt();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_INT) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        new Test(byte.class, n -> 0xFF & n.byteValue(), 0xFF & QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> 0xFFFF & n.intValue(), 0xFFFF & QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, n -> 0xFFFF & n.shortValue(), 0xFFFF & QueryConstants.NULL_SHORT).runTest();
        new Test(int.class).runTest();
        new Test(long.class).runTest();
        new Test(float.class, n -> (int) n.floatValue(), (int) QueryConstants.NULL_FLOAT).runTest();
        new Test(double.class).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testInt64() throws Exception {
        class Test extends IntRoundTripTest<BigIntVector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super(BigIntVector::get, QueryConstants.NULL_LONG, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(64, true), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final BigIntVector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_LONG, QueryConstants.MAX_LONG, -1L, 0L, 1L);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    long value = rnd.nextLong();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_LONG) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        new Test(byte.class, Number::byteValue, QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> (long) (char) n.intValue(), QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, Number::shortValue, QueryConstants.NULL_SHORT).runTest();
        new Test(int.class, Number::intValue, QueryConstants.NULL_INT).runTest();
        new Test(long.class).runTest();
        new Test(float.class, n -> (long) n.floatValue(), (long) QueryConstants.NULL_FLOAT).runTest();
        new Test(double.class, Number::doubleValue, (long) QueryConstants.NULL_DOUBLE).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testUint64() throws Exception {
        class Test extends IntRoundTripTest<UInt8Vector> {
            Test(Class<?> dhType) {
                this(dhType, null, 0);
            }

            Test(Class<?> dhType, Function<Number, Number> truncate, long dhWireNull) {
                super(UInt8Vector::get, QueryConstants.NULL_LONG, dhType, truncate, dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, new ArrowType.Int(64, false), dhType);
            }

            @Override
            public int initializeRoot(@NotNull final UInt8Vector source) {
                int start = setAll(source::set,
                        QueryConstants.MIN_LONG, QueryConstants.MAX_LONG, -1L, 0L, 1L);
                for (int ii = start; ii < NUM_ROWS; ++ii) {
                    long value = rnd.nextLong();
                    source.set(ii, value);
                    if (value == QueryConstants.NULL_LONG) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }
        }

        new Test(byte.class, n -> 0xFF & n.byteValue(), 0xFF & QueryConstants.NULL_BYTE).runTest();
        new Test(char.class, n -> 0xFFFF & n.intValue(), 0xFFFF & QueryConstants.NULL_CHAR).runTest();
        new Test(short.class, n -> 0xFFFF & n.shortValue(), 0xFFFF & QueryConstants.NULL_SHORT).runTest();
        new Test(int.class, n -> 0xFFFFFFFFL & n.intValue(), 0xFFFFFFFFL & QueryConstants.NULL_INT).runTest();
        new Test(long.class).runTest();
        new Test(float.class, n -> (long) n.floatValue(), (long) QueryConstants.NULL_FLOAT).runTest();
        new Test(double.class, n -> (long) n.doubleValue(), (long) QueryConstants.NULL_DOUBLE).runTest();
        new Test(BigInteger.class).runTest();
        new Test(BigDecimal.class).runTest();
    }

    @Test
    public void testBit() throws Exception {
        // note that dh does not support primitive boolean columns because there would be no way to represent null
        new BoolRoundTripTest(Boolean.class).runTest();
        new BoolRoundTripTest(byte.class).runTest();
        for (TestArrayMode arrayMode : TestArrayMode.values()) {
            if (arrayMode == TestArrayMode.NONE || arrayMode.isVector()) {
                continue;
            }
            new BoolRoundTripTest(boolean.class).runTest(TestNullMode.NOT_NULLABLE, arrayMode);
        }
    }

    @Test
    public void testDecimal128() throws Exception {
        // 128-bit tests
        new DecimalRoundTripTest(BigDecimal.class, 1, 0).runTest();
        new DecimalRoundTripTest(BigDecimal.class, 19, 0).runTest();
        new DecimalRoundTripTest(BigDecimal.class, 19, 9).runTest();
        new DecimalRoundTripTest(BigDecimal.class, 38, 0).runTest();
        new DecimalRoundTripTest(BigDecimal.class, 38, 19).runTest();
        new DecimalRoundTripTest(BigDecimal.class, 38, 37).runTest();

        // test dh coercion
        new DecimalRoundTripTest(byte.class, QueryConstants.MIN_BYTE, QueryConstants.MAX_BYTE, true).runTest();
        new DecimalRoundTripTest(char.class, QueryConstants.MIN_CHAR, QueryConstants.MAX_CHAR, true).runTest();
        new DecimalRoundTripTest(short.class, QueryConstants.MIN_SHORT, QueryConstants.MAX_SHORT, true).runTest();
        new DecimalRoundTripTest(int.class, QueryConstants.MIN_INT, QueryConstants.MAX_INT, true).runTest();
        new DecimalRoundTripTest(long.class, QueryConstants.MIN_LONG, QueryConstants.MAX_LONG, true).runTest();

        final int floatDigits = (int) Math.floor(Math.log10(1L << 24));
        new DecimalRoundTripTest(float.class, floatDigits, floatDigits / 2).runTest();
        final int doubleDigits = (int) Math.floor(Math.log10(1L << 53));
        new DecimalRoundTripTest(double.class, doubleDigits, doubleDigits / 2).runTest();
    }

    @Test
    public void testDecimal256() throws Exception {
        // 256-bit tests
        new Decimal256RoundTripTest(BigDecimal.class, 1, 0).runTest();
        new Decimal256RoundTripTest(BigDecimal.class, 38, 0).runTest();
        new Decimal256RoundTripTest(BigDecimal.class, 38, 19).runTest();
        new Decimal256RoundTripTest(BigDecimal.class, 76, 0).runTest();
        new Decimal256RoundTripTest(BigDecimal.class, 76, 38).runTest();
        new Decimal256RoundTripTest(BigDecimal.class, 76, 75).runTest();

        // test dh coercion
        new Decimal256RoundTripTest(byte.class, QueryConstants.MIN_BYTE, QueryConstants.MAX_BYTE, true).runTest();
        new Decimal256RoundTripTest(char.class, QueryConstants.MIN_CHAR, QueryConstants.MAX_CHAR, true).runTest();
        new Decimal256RoundTripTest(short.class, QueryConstants.MIN_SHORT, QueryConstants.MAX_SHORT, true).runTest();
        new Decimal256RoundTripTest(int.class, QueryConstants.MIN_INT, QueryConstants.MAX_INT, true).runTest();
        new Decimal256RoundTripTest(long.class, QueryConstants.MIN_LONG, QueryConstants.MAX_LONG, true).runTest();

        final int floatDigits = (int) Math.floor(Math.log10(1L << 24));
        new DecimalRoundTripTest(float.class, floatDigits, floatDigits / 2).runTest();
        final int doubleDigits = (int) Math.floor(Math.log10(1L << 53));
        new DecimalRoundTripTest(double.class, doubleDigits, doubleDigits / 2).runTest();
    }

    // For list tests: test both head and tail via FixedSizeList limits
    // Union needs to test boolean transformation

    @SafeVarargs
    private static <T> int setAll(BiConsumer<Integer, T> setter, T... values) {
        for (int ii = 0; ii < values.length; ++ii) {
            setter.accept(ii, values[ii]);
        }
        return values.length;
    }

    protected enum TestNullMode {
        EMPTY, ALL, NONE, SOME, NOT_NULLABLE
    }
    protected enum TestArrayMode {
        NONE, FIXED_ARRAY, VAR_ARRAY, VIEW_ARRAY, FIXED_VECTOR, VAR_VECTOR, VIEW_VECTOR;

        boolean isVector() {
            switch (this) {
                case FIXED_VECTOR:
                case VAR_VECTOR:
                case VIEW_VECTOR:
                    return true;
                default:
                    return false;
            }
        }

        boolean isVariableLength() {
            switch (this) {
                case VAR_ARRAY:
                case VAR_VECTOR:
                    return true;
                default:
                    return false;
            }
        }

        boolean isView() {
            switch (this) {
                case VIEW_ARRAY:
                case VIEW_VECTOR:
                    return true;
                default:
                    return false;
            }
        }
    }

    private static ArrowType getArrayArrowType(final TestArrayMode mode) {
        switch (mode) {
            case FIXED_ARRAY:
            case FIXED_VECTOR:
                return new ArrowType.FixedSizeList(MAX_LIST_ITEM_LEN);
            case VAR_ARRAY:
            case VAR_VECTOR:
                return new ArrowType.List();
            case VIEW_ARRAY:
            case VIEW_VECTOR:
                return new ArrowType.ListView();
            default:
                throw new IllegalArgumentException("Unexpected array mode: " + mode);
        }
    }

    private abstract class RoundTripTest<T extends FieldVector> {
        protected final Random rnd = new Random(RANDOM_SEED);
        protected Class<?> dhType;
        protected Class<?> componentType;

        public RoundTripTest(@NotNull final Class<?> dhType) {
            this(dhType, null);
        }

        public RoundTripTest(@NotNull final Class<?> dhType, @Nullable final Class<?> componentType) {
            this.dhType = dhType;
            this.componentType = componentType;
        }

        public abstract Schema newSchema(boolean isNullable);

        public abstract int initializeRoot(@NotNull T source);

        public abstract void validate(TestNullMode nullMode, @NotNull T source, @NotNull T dest);

        public void runTest() throws Exception {
            for (TestArrayMode arrayMode : TestArrayMode.values()) {
                for (TestNullMode mode : TestNullMode.values()) {
                    runTest(mode, arrayMode);
                }
            }
        }

        public void runTest(final TestNullMode nullMode, final TestArrayMode arrayMode) throws Exception {
            final boolean isNullable = nullMode != TestNullMode.NOT_NULLABLE;
            final int listItemLength;
            Schema schema = newSchema(isNullable);

            if (arrayMode == TestArrayMode.NONE) {
                listItemLength = 0;
            } else {
                final Field innerField = schema.getFields().get(0);

                final Map<String, String> attrs = new LinkedHashMap<>(innerField.getMetadata());
                attrs.put(DH_COMPONENT_TYPE_TAG, innerField.getMetadata().get(DH_TYPE_TAG));
                if (arrayMode.isVector()) {
                    final Class<?> vectorType = VectorFactory.forElementType(dhType).vectorType();
                    attrs.put(DH_TYPE_TAG, vectorType.getCanonicalName());
                } else {
                    attrs.put(DH_TYPE_TAG, innerField.getMetadata().get(DH_TYPE_TAG) + "[]");
                }

                final ArrowType listType = getArrayArrowType(arrayMode);
                final FieldType fieldType = new FieldType(isNullable, listType, null, attrs);
                schema = new Schema(Collections.singletonList(
                        new Field(COLUMN_NAME, fieldType, Collections.singletonList(innerField))));

                if (listType.getTypeID() == ArrowType.FixedSizeList.TYPE_TYPE) {
                    listItemLength = ((ArrowType.FixedSizeList) listType).getListSize();
                } else {
                    listItemLength = 0;
                }
            }

            try (final VectorSchemaRoot source = VectorSchemaRoot.create(schema, allocator)) {
                source.allocateNew();
                final FieldVector dataVector = getDataVector(arrayMode, source, listItemLength);

                if (nullMode == TestNullMode.EMPTY) {
                    source.setRowCount(0);
                } else {
                    // pre-allocate buffers
                    source.setRowCount(NUM_ROWS);

                    // noinspection unchecked
                    int numRows = initializeRoot((T) dataVector);
                    source.setRowCount(numRows);

                    if (nullMode == TestNullMode.ALL) {
                        for (int ii = 0; ii < source.getRowCount(); ++ii) {
                            dataVector.setNull(ii);
                        }
                    } else if (nullMode == TestNullMode.SOME) {
                        for (int ii = 0; ii < source.getRowCount(); ++ii) {
                            if (rnd.nextBoolean()) {
                                dataVector.setNull(ii);
                            }
                        }
                    }

                    if (arrayMode != TestArrayMode.NONE) {
                        if (listItemLength != 0) {
                            int realRows = numRows / listItemLength;
                            dataVector.setValueCount(realRows * listItemLength);
                            for (int ii = 0; ii < realRows; ++ii) {
                                FixedSizeListVector listVector = (FixedSizeListVector) source.getVector(0);
                                if (isNullable && rnd.nextBoolean()) {
                                    listVector.setNull(ii);
                                    // to simplify validation, set inner values to null
                                    for (int jj = 0; jj < listItemLength; ++jj) {
                                        listVector.getDataVector().setNull(ii * listItemLength + jj);
                                    }
                                } else {
                                    listVector.setNotNull(ii);
                                }
                            }
                            source.setRowCount(realRows);
                        } else if (arrayMode.isVariableLength()) {
                            int itemsConsumed = 0;
                            final ListVector listVector = (ListVector) source.getVector(0);
                            for (int ii = 0; ii < numRows; ++ii) {
                                if (isNullable && rnd.nextBoolean()) {
                                    listVector.setNull(ii);
                                    continue;
                                } else if (rnd.nextInt(8) == 0) {
                                    listVector.startNewValue(ii);
                                    listVector.endValue(ii, 0);
                                    continue;
                                }
                                int itemLen = Math.min(rnd.nextInt(MAX_LIST_ITEM_LEN), numRows - itemsConsumed);
                                listVector.startNewValue(itemsConsumed);
                                listVector.endValue(itemsConsumed, itemLen);
                                itemsConsumed += itemLen;
                            }
                            dataVector.setValueCount(itemsConsumed);
                        } else {
                            final ListViewVector listVector = (ListViewVector) source.getVector(0);
                            dataVector.setValueCount(numRows);
                            int maxItemWritten = 0;
                            for (int ii = 0; ii < numRows; ++ii) {
                                if (isNullable && rnd.nextBoolean()) {
                                    listVector.setNull(ii);
                                    continue;
                                }
                                int sPos = rnd.nextInt(numRows);
                                int itemLen = rnd.nextInt(Math.min(MAX_LIST_ITEM_LEN, numRows - sPos));
                                listVector.setValidity(ii, 1);
                                listVector.setOffset(ii, sPos);
                                listVector.setSize(ii, itemLen);
                                maxItemWritten = Math.max(maxItemWritten, sPos + itemLen);
                            }
                            dataVector.setValueCount(maxItemWritten);
                        }
                    }
                }

                int flightDescriptorTicketValue = nextTicket++;
                FlightDescriptor descriptor = FlightDescriptor.path("export", flightDescriptorTicketValue + "");
                FlightClient.ClientStreamListener putStream =
                        flightClient.startPut(descriptor, source, new AsyncPutListener());
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
                if (arrayMode == TestArrayMode.NONE) {
                    assertEquals(dhType, columnSource.getType());
                    assertEquals(componentType, columnSource.getComponentType());
                } else {
                    if (arrayMode.isVector()) {
                        assertTrue(io.deephaven.vector.Vector.class.isAssignableFrom(columnSource.getType()));
                    } else {
                        assertTrue(columnSource.getType().isArray());
                        assertEquals(dhType, columnSource.getType().getComponentType());
                    }
                    assertEquals(dhType, columnSource.getComponentType());
                }

                try (FlightStream stream = flightClient.getStream(flightTicketFor(flightDescriptorTicketValue))) {
                    VectorSchemaRoot dest = stream.getRoot();

                    int numPayloads = 0;
                    while (stream.next()) {
                        assertEquals(source.getRowCount(), dest.getRowCount());

                        if (arrayMode != TestArrayMode.NONE) {
                            validateList(arrayMode, source.getVector(0), dest.getVector(0));
                        }

                        if (arrayMode == TestArrayMode.NONE) {
                            // noinspection unchecked
                            validate(nullMode, (T) dataVector, (T) getDataVector(arrayMode, dest, listItemLength));
                        } else if (arrayMode.isView()) {
                            // TODO: rm this branch when https://github.com/apache/arrow-java/issues/471 is fixed

                            // DH will unwrap the view, so to validate the data vector we need to unwrap it as well
                            try (final ListViewVector newView =
                                    (ListViewVector) schema.getFields().get(0).createVector(allocator)) {
                                newView.setValueCount(source.getRowCount());
                                final ListViewVector sourceArr = (ListViewVector) source.getVector(0);
                                int totalLen = 0;
                                for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                    if (!sourceArr.isNull(ii)) {
                                        // TODO: when https://github.com/apache/arrow-java/issues/470 is fixed, use
                                        // totalLen += sourceArr.getElementEndIndex(ii) -
                                        // sourceArr.getElementStartIndex(ii);
                                        totalLen += sourceArr.getObject(ii).size();
                                    }
                                }
                                Assert.geqZero(totalLen, "totalLen");

                                newView.getDataVector().setValueCount(totalLen);
                                for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                    if (sourceArr.isNull(ii)) {
                                        newView.setNull(ii);
                                    } else {
                                        copyListItem(newView, sourceArr, ii);
                                    }
                                }

                                // if the inner data is empty then we the inner DataVector will be a ZeroVector not a T
                                if (totalLen != 0) {
                                    // noinspection unchecked
                                    validate(nullMode, (T) newView.getDataVector(),
                                            (T) getDataVector(arrayMode, dest, listItemLength));
                                }
                            }
                        } else {
                            // any null values will not be sent back, so we need to filter the source to match
                            try (final BaseListVector newView =
                                    (BaseListVector) schema.getFields().get(0).createVector(allocator)) {
                                newView.setValueCount(source.getRowCount());
                                final BaseListVector sourceArr = (BaseListVector) source.getVector(0);
                                int totalLen = 0;
                                for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                    if (!sourceArr.isNull(ii)) {
                                        totalLen +=
                                                sourceArr.getElementEndIndex(ii) - sourceArr.getElementStartIndex(ii);
                                    }
                                }
                                Assert.geqZero(totalLen, "totalLen");

                                final int finTotalLen = totalLen;
                                newView.getChildrenFromFields().forEach(c -> c.setValueCount(finTotalLen));
                                for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                    if (sourceArr.isNull(ii)) {
                                        newView.setNull(ii);
                                    } else {
                                        newView.copyFrom(ii, ii, sourceArr);
                                    }
                                }

                                // if the inner data is empty then we the inner DataVector will be a ZeroVector not a T
                                if (totalLen != 0) {
                                    // noinspection unchecked
                                    validate(nullMode, (T) newView.getChildrenFromFields().get(0),
                                            (T) getDataVector(arrayMode, dest, listItemLength));
                                }
                            }
                        }
                        ++numPayloads;
                    }

                    // if there is data, we should be able to encode in a single payload
                    assertEquals(nullMode == TestNullMode.EMPTY ? 0 : 1, numPayloads);
                }
            }
        }
    }

    private static void copyListItem(
            @NotNull final ListViewVector dest,
            @NotNull final ListViewVector source,
            final int index) {
        Preconditions.checkArgument(dest.getMinorType() == source.getMinorType());
        FieldReader in = source.getReader();
        in.setPosition(index);
        FieldWriter out = dest.getWriter();
        out.setPosition(index);

        if (!in.isSet()) {
            out.writeNull();
            return;
        }

        out.startList();
        FieldReader childReader = in.reader();
        FieldWriter childWriter = getListWriterForReader(childReader, out);
        for (int ii = 0; ii < in.size(); ++ii) {
            childReader.setPosition(source.getElementStartIndex(index) + ii);
            if (childReader.isSet()) {
                ComplexCopier.copy(childReader, childWriter);
            } else {
                childWriter.writeNull();
            }
        }
        out.endList();
    }

    private static FieldWriter getListWriterForReader(
            @NotNull final FieldReader reader,
            @NotNull final BaseWriter.ListWriter writer) {
        switch (reader.getMinorType()) {
            case TINYINT:
                return (FieldWriter) writer.tinyInt();
            case UINT1:
                return (FieldWriter) writer.uInt1();
            case UINT2:
                return (FieldWriter) writer.uInt2();
            case SMALLINT:
                return (FieldWriter) writer.smallInt();
            case FLOAT2:
                return (FieldWriter) writer.float2();
            case INT:
                return (FieldWriter) writer.integer();
            case UINT4:
                return (FieldWriter) writer.uInt4();
            case FLOAT4:
                return (FieldWriter) writer.float4();
            case DATEDAY:
                return (FieldWriter) writer.dateDay();
            case INTERVALYEAR:
                return (FieldWriter) writer.intervalYear();
            case TIMESEC:
                return (FieldWriter) writer.timeSec();
            case TIMEMILLI:
                return (FieldWriter) writer.timeMilli();
            case BIGINT:
                return (FieldWriter) writer.bigInt();
            case UINT8:
                return (FieldWriter) writer.uInt8();
            case FLOAT8:
                return (FieldWriter) writer.float8();
            case DATEMILLI:
                return (FieldWriter) writer.dateMilli();
            case TIMESTAMPSEC:
                return (FieldWriter) writer.timeStampSec();
            case TIMESTAMPMILLI:
                return (FieldWriter) writer.timeStampMilli();
            case TIMESTAMPMICRO:
                return (FieldWriter) writer.timeStampMicro();
            case TIMESTAMPNANO:
                return (FieldWriter) writer.timeStampNano();
            case TIMEMICRO:
                return (FieldWriter) writer.timeMicro();
            case TIMENANO:
                return (FieldWriter) writer.timeNano();
            case INTERVALDAY:
                return (FieldWriter) writer.intervalDay();
            case INTERVALMONTHDAYNANO:
                return (FieldWriter) writer.intervalMonthDayNano();
            case DECIMAL256:
                return (FieldWriter) writer.decimal256();
            case DECIMAL:
                return (FieldWriter) writer.decimal();
            case VARBINARY:
                return (FieldWriter) writer.varBinary();
            case VARCHAR:
                return (FieldWriter) writer.varChar();
            case VIEWVARBINARY:
                return (FieldWriter) writer.viewVarBinary();
            case VIEWVARCHAR:
                return (FieldWriter) writer.viewVarChar();
            case LARGEVARCHAR:
                return (FieldWriter) writer.largeVarChar();
            case LARGEVARBINARY:
                return (FieldWriter) writer.largeVarBinary();
            case BIT:
                return (FieldWriter) writer.bit();
            case STRUCT:
                return (FieldWriter) writer.struct();
            case FIXED_SIZE_LIST:
            case LIST:
            case MAP:
            case NULL:
                return (FieldWriter) writer.list();
            case LISTVIEW:
                return (FieldWriter) writer.listView();
            default:
                throw new UnsupportedOperationException(reader.getMinorType().toString());
        }
    }

    private static void validateList(
            final TestArrayMode arrayMode,
            final FieldVector source,
            final FieldVector dest) {}

    private static FieldVector getDataVector(
            final TestArrayMode arrayMode,
            final VectorSchemaRoot source,
            final int listItemLength) {
        if (arrayMode == TestArrayMode.NONE) {
            return source.getVector(0);
        } else {
            if (listItemLength != 0) {
                final FixedSizeListVector arrayVector = (FixedSizeListVector) source.getVector(0);
                return arrayVector.getDataVector();
            } else if (arrayMode.isVariableLength()) {
                final ListVector arrayVector = (ListVector) source.getVector(0);
                return arrayVector.getDataVector();
            } else {
                final ListViewVector arrayVector = (ListViewVector) source.getVector(0);
                return arrayVector.getDataVector();
            }
        }
    }

    private abstract class IntRoundTripTest<T extends FieldVector> extends RoundTripTest<T> {
        private final BiFunction<T, Integer, Number> getter;
        private final long dhSourceNull;
        private final Function<Number, Number> truncate;
        private final long dhWireNull;

        public IntRoundTripTest(
                @NotNull BiFunction<T, Integer, Number> getter,
                long dhSourceNull,
                @NotNull Class<?> dhType,
                @Nullable Function<Number, Number> truncate,
                long dhWireNull) {
            super(dhType);
            this.getter = getter;
            this.dhSourceNull = dhSourceNull;
            this.truncate = truncate;
            this.dhWireNull = dhWireNull;
        }

        @Override
        public void validate(final TestNullMode nullMode, @NotNull final T source, @NotNull final T dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                    continue;
                } else if (truncate == null) {
                    assertEquals(getter.apply(source, ii), getter.apply(dest, ii));
                    continue;
                }

                final long truncated = truncate.apply(getter.apply(source, ii)).longValue();
                if (truncated == dhWireNull || truncated == dhSourceNull) {
                    if (nullMode == TestNullMode.NOT_NULLABLE) {
                        assertEquals(getter.apply(dest, ii).longValue(), dhSourceNull);
                    } else {
                        assertTrue(dest.isNull(ii));
                    }
                } else {
                    assertEquals(truncated, getter.apply(dest, ii).longValue());
                }
            }
        }
    }

    private class BoolRoundTripTest extends RoundTripTest<BitVector> {
        public BoolRoundTripTest(@NotNull Class<?> dhType) {
            super(dhType);
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, new ArrowType.Bool(), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final BitVector source) {
            int start = setAll(source::set, 1, 0);
            for (int ii = start; ii < NUM_ROWS; ++ii) {
                boolean value = rnd.nextBoolean();
                source.set(ii, value ? 1 : 0);
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(final TestNullMode nullMode, @NotNull final BitVector source,
                @NotNull final BitVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.getValueCount() <= ii || dest.isNull(ii));
                } else {
                    assertEquals(source.get(ii), dest.get(ii));
                }
            }
        }
    }

    private static BigDecimal randomBigDecimal(Random rnd, int precision, int scale) {
        // reduce precision some of the time to improve coverage
        if (rnd.nextInt(10) == 0) {
            precision = rnd.nextInt(precision);
        }

        // The number of bits needed is roughly log2(10^precision); or ~3.3 * precision.
        BigInteger unscaled = new BigInteger(precision * 3 + 3, rnd).abs();

        // If it somehow exceeds 10^precision, mod it down
        final BigInteger limit = BigInteger.TEN.pow(precision);
        unscaled = unscaled.mod(limit);

        if (rnd.nextBoolean()) {
            unscaled = unscaled.negate();
        }

        return new BigDecimal(unscaled, scale);
    }

    private class DecimalRoundTripTest extends RoundTripTest<DecimalVector> {
        final private int precision;
        final private int scale;
        final private long minValue;
        final private long maxValue;

        public DecimalRoundTripTest(
                @NotNull Class<?> dhType, long precision, long scale) {
            this(dhType, precision, scale, false);
        }

        public DecimalRoundTripTest(
                @NotNull Class<?> dhType, long precision, long scale, boolean primitiveDest) {
            super(dhType);

            if (primitiveDest) {
                this.minValue = precision;
                this.maxValue = scale;
                this.precision = (int) Math.ceil(Math.log10(maxValue));
                this.scale = 0;
            } else {
                this.minValue = 0;
                this.maxValue = 0;
                this.precision = (int) precision;
                this.scale = (int) scale;
            }
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, new ArrowType.Decimal(precision, scale, 128), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final DecimalVector source) {
            if (maxValue != 0) {
                final BigInteger range = BigInteger.valueOf(maxValue).subtract(BigInteger.valueOf(minValue));
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    final BigInteger nextValue = new BigInteger(range.bitLength(), rnd)
                            .mod(range).add(BigInteger.valueOf(minValue));
                    source.set(ii, nextValue.longValue());
                }
            } else {
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    source.set(ii, randomBigDecimal(rnd, precision, scale));
                }
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(final TestNullMode nullMode, @NotNull final DecimalVector source,
                @NotNull final DecimalVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private class Decimal256RoundTripTest extends RoundTripTest<Decimal256Vector> {
        final private int precision;
        final private int scale;
        final private long minValue;
        final private long maxValue;

        public Decimal256RoundTripTest(
                @NotNull Class<?> dhType, long precision, long scale) {
            this(dhType, precision, scale, false);
        }

        public Decimal256RoundTripTest(
                @NotNull Class<?> dhType, long precision, long scale, boolean primitiveDest) {
            super(dhType);

            if (primitiveDest) {
                this.minValue = precision;
                this.maxValue = scale;
                this.precision = (int) Math.ceil(Math.log10(maxValue));
                this.scale = 0;
            } else {
                this.minValue = 0;
                this.maxValue = 0;
                this.precision = (int) precision;
                this.scale = (int) scale;
            }
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, new ArrowType.Decimal(precision, scale, 256), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final Decimal256Vector source) {
            if (maxValue != 0) {
                final BigInteger range = BigInteger.valueOf(maxValue).subtract(BigInteger.valueOf(minValue));
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    final BigInteger nextValue = new BigInteger(range.bitLength(), rnd)
                            .mod(range).add(BigInteger.valueOf(minValue));
                    source.set(ii, nextValue.longValue());
                }
            } else {
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    source.set(ii, randomBigDecimal(rnd, precision, scale));
                }
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(
                final TestNullMode nullMode,
                @NotNull final Decimal256Vector source,
                @NotNull final Decimal256Vector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private static Ticket flightTicketFor(int flightDescriptorTicketValue) {
        return new Ticket(FlightExportTicketHelper.exportIdToFlightTicket(flightDescriptorTicketValue).getTicket()
                .toByteArray());
    }
}
