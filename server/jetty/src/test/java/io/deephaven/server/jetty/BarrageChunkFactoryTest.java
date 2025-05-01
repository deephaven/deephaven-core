//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AuthContext;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.client.impl.BearerHandler;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionConfig;
import io.deephaven.client.impl.SessionImpl;
import io.deephaven.client.impl.SessionImplConfig;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.chunk.LongChunkReader;
import io.deephaven.extensions.barrage.chunk.LongChunkWriter;
import io.deephaven.extensions.barrage.util.ArrowIpcUtil;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.plugin.Registration;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.arrow.ExchangeMarshallerModule;
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
import io.deephaven.server.test.TestAuthModule;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.ByteVector;
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
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.AbstractContainerVector;
import org.apache.arrow.vector.complex.BaseListVector;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.complex.writer.FixedSizeBinaryWriter;
import org.apache.arrow.vector.holders.NullableDurationHolder;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
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
import javax.inject.Provider;
import javax.inject.Singleton;
import java.io.DataInput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BarrageChunkFactoryTest {
    private static final String COLUMN_NAME = "test_col";
    private static final int NUM_ROWS = 1023;
    private static final int RANDOM_SEED = 42;
    private static final int MAX_LIST_ITEM_LEN = 5;
    private static final int HEAD_LIST_ITEM_LEN = 2;

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
                    .allowedHttpMethods(Set.of("POST"))
                    .build();
        }
    }

    @Singleton
    @Component(modules = {
            ExecutionContextUnitTestModule.class,
            FlightTestModule.class,
            JettyServerModule.class,
            JettyTestConfig.class,
            ExchangeMarshallerModule.class,
    })
    public interface JettyTestComponent extends TestComponent {
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
        @Singleton
        Scheduler provideScheduler() {
            return new Scheduler.DelegatingImpl(
                    Executors.newSingleThreadExecutor(
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("test-scheduler-single-%d")
                                    .build()),
                    Executors.newScheduledThreadPool(1,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("test-scheduler-multi-%d")
                                    .build()),
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

        Scheduler serverScheduler();

        Provider<ScriptSession> scriptSessionProvider();
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
    private TestComponent component;
    private Scheduler.DelegatingImpl serverScheduler;

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

        component = DaggerBarrageChunkFactoryTest_JettyTestComponent.create();
        // open execution context immediately so it can be used when resolving `scriptSession`
        executionContext = component.executionContext().open();

        server = component.server();
        server.start();
        serverScheduler = (Scheduler.DelegatingImpl) component.serverScheduler();
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

        clientScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("test-client-scheduler-%d").build());

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
    public void teardown() throws InterruptedException {
        component.scriptSessionProvider().get().cleanup();

        clientSession.close();
        clientScheduler.shutdownNow();
        clientChannel.shutdownNow();

        sessionService.closeAllSessions();
        executionContext.close();

        closeClient();
        server.beginShutdown();
        server.stopWithTimeout(1, java.util.concurrent.TimeUnit.MINUTES);
        serverScheduler.shutdown();

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

    private Schema createSchema(boolean nullable, boolean isDefault, ArrowType arrowType, Class<?> dhType) {
        return createSchema(nullable, isDefault, arrowType, dhType, null);
    }

    private Schema createSchema(
            final boolean nullable,
            final boolean isDefault,
            final ArrowType arrowType,
            final Class<?> dhType,
            final Class<?> dhComponentType) {
        final Map<String, String> attrs;
        if (isDefault) {
            attrs = null;
        } else {
            attrs = new HashMap<>();
            attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG, dhType.getCanonicalName());
            if (dhComponentType != null) {
                attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_COMPONENT_TYPE_TAG,
                        dhComponentType.getCanonicalName());
            }
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

    private static long factorForTimeUnit(final TimeUnit unit) {
        return switch (unit) {
            case NANOSECOND -> 1L;
            case MICROSECOND -> 1000L;
            case MILLISECOND -> 1000 * 1000L;
            case SECOND -> 1000 * 1000 * 1000L;
        };
    }

    @Test
    public void testRegisterCustomType() throws Exception {
        // this permanently registers the custom types, but Duration -> Long is not the default mapping
        DefaultChunkReaderFactory.INSTANCE.register(
                ArrowType.ArrowTypeID.Duration,
                long.class,
                (arrowType, typeInfo, options) -> {
                    final long factor = factorForTimeUnit(((ArrowType.Duration) arrowType).getUnit());
                    return new LongChunkReader(options) {
                        @Override
                        public WritableLongChunk<Values> readChunk(
                                @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
                                @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
                                @NotNull final DataInput is,
                                @Nullable final WritableChunk<Values> outChunk,
                                final int outOffset,
                                final int totalRows) throws IOException {
                            final WritableLongChunk<Values> values =
                                    super.readChunk(fieldNodeIter, bufferInfoIter, is, outChunk, outOffset, totalRows);
                            for (int ii = outOffset; ii < values.size(); ++ii) {
                                if (!values.isNull(ii)) {
                                    values.set(ii, values.get(ii) * factor);
                                }
                            }
                            return values;
                        }
                    };
                });

        DefaultChunkWriterFactory.INSTANCE.register(
                ArrowType.ArrowTypeID.Duration,
                long.class,
                typeInfo -> {
                    final long factor =
                            factorForTimeUnit(((ArrowType.Duration) typeInfo.arrowField().getType()).getUnit());
                    return new LongChunkWriter<>((source) -> {
                        final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(source.size());
                        for (int ii = 0; ii < source.size(); ++ii) {
                            final long value = source.get(ii); // assume in nanoseconds
                            chunk.set(ii,
                                    value == QueryConstants.NULL_LONG ? QueryConstants.NULL_LONG : value / factor);
                        }
                        return chunk;
                    }, LongChunk::getEmptyChunk, typeInfo.arrowField().isNullable());
                });

        class Test extends RoundTripTest<DurationVector> {
            private final TimeUnit timeUnit;

            Test(final TimeUnit timeUnit) {
                super(long.class);
                this.timeUnit = timeUnit;
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Duration(timeUnit), dhType);
            }

            @Override
            public int initializeRoot(@NotNull DurationVector source) {
                final long factor = factorForTimeUnit(timeUnit);
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    final long nextValue = rnd.nextLong() / factor;
                    source.set(ii, nextValue);
                    if (nextValue == QueryConstants.NULL_LONG) {
                        --ii;
                    }
                }
                return NUM_ROWS;
            }

            @Override
            public void validate(TestNullMode nullMode, @NotNull DurationVector source, @NotNull DurationVector dest) {
                for (int ii = 0; ii < source.getValueCount(); ++ii) {
                    if (source.isNull(ii)) {
                        assertTrue(dest.isNull(ii));
                    } else {
                        assertEquals(source.getObject(ii), dest.getObject(ii));
                    }
                }
            }
        }

        new Test(TimeUnit.NANOSECOND).runTest();
        new Test(TimeUnit.MILLISECOND).runTest();
        new Test(TimeUnit.MICROSECOND).runTest();
        new Test(TimeUnit.SECOND).runTest();
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
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(8, true), dhType);
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

        // note that byte[] defaults to VarBinary rather than VarList as byte arrays often have special treatment
        new Test(byte.class, Number::byteValue, QueryConstants.NULL_BYTE).isDefault().runTest();
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
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(8, false), dhType);
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
        new Test(short.class, Number::shortValue, QueryConstants.NULL_SHORT).isDefaultUpload().runTest();
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
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(16, true), dhType);
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
        new Test(short.class, Number::shortValue, QueryConstants.NULL_SHORT).isDefault().runTest();
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
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(16, false), dhType);
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
        new Test(char.class, n -> (int) (char) n.intValue(), (int) QueryConstants.NULL_CHAR).isDefaultUpload()
                .runTest();
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
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(32, true), dhType);
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
        new Test(int.class).isDefault().runTest();
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
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(32, false), dhType);
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
        new Test(long.class).isDefaultUpload().runTest();
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
                super((v, ii) -> (v.isNull(ii) ? null : v.get(ii)), QueryConstants.NULL_LONG, dhType, truncate,
                        dhWireNull);
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(64, true), dhType);
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
        new Test(long.class).isDefault().runTest();
        new Test(float.class, n -> (long) n.floatValue(), (long) QueryConstants.NULL_FLOAT).runTest();
        new Test(double.class, Number::doubleValue, (long) QueryConstants.NULL_DOUBLE).runTest();
        new Test(BigInteger.class, Number::longValue, QueryConstants.NULL_LONG).runTest();
        new Test(BigDecimal.class, Number::longValue, QueryConstants.NULL_LONG).runTest();
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
                return createSchema(isNullable, isDefaultUpload, new ArrowType.Int(64, false), dhType);
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
        // Note that BigInteger is the default for upload, but default download is our custom encoding of BigInteger
        new Test(BigInteger.class, Number::longValue, QueryConstants.NULL_LONG).runTest();
        new Test(BigDecimal.class, Number::longValue, QueryConstants.NULL_LONG).runTest();
    }

    @Test
    public void testBit() throws Exception {
        // note that dh does not support primitive boolean columns because there would be no way to represent null
        new BoolRoundTripTest(Boolean.class).runTest();
        new BoolRoundTripTest(byte.class).runTest();
        for (TestWrapMode wrapMode : TestWrapMode.values()) {
            if (wrapMode == TestWrapMode.NONE || wrapMode.isVector() || wrapMode == TestWrapMode.MAP_KEY) {
                continue;
            }
            new BoolRoundTripTest(boolean.class).runTest(wrapMode, TestNullMode.NOT_NULLABLE);
        }
    }

    @Test
    public void testDecimal128() throws Exception {
        // 128-bit tests
        for (int scale : new int[] {0, 9, 18, 27, 36}) {
            if (scale < 1) {
                // is default for upload only as we custom encode BigDecimal by default
                new DecimalRoundTripTest(BigDecimal.class, 1, scale).isDefaultUpload().runTest();
                new DecimalRoundTripTest(BigInteger.class, 1, scale).runTest();
            }
            if (scale < 19) {
                new DecimalRoundTripTest(BigDecimal.class, 19, scale).runTest();
                new DecimalRoundTripTest(BigInteger.class, 19, scale).skipMapKey().runTest();
            }
            if (scale < 38) {
                new DecimalRoundTripTest(BigDecimal.class, 38, scale).runTest();
                new DecimalRoundTripTest(BigInteger.class, 38, scale).skipMapKey().runTest();
            }
        }

        // test dh coercion
        new DecimalRoundTripTest(byte.class).runTest();
        new DecimalRoundTripTest(char.class).runTest();
        new DecimalRoundTripTest(short.class).runTest();
        new DecimalRoundTripTest(int.class).runTest();
        new DecimalRoundTripTest(long.class).runTest();

        final int floatDigits = (int) Math.floor(Math.log10(1L << 24));
        new DecimalRoundTripTest(float.class, floatDigits, floatDigits / 2).runTest();
        final int doubleDigits = (int) Math.floor(Math.log10(1L << 53));
        new DecimalRoundTripTest(double.class, doubleDigits, doubleDigits / 2).runTest();
    }

    @Test
    public void testDecimal256() throws Exception {
        // 256-bit tests
        for (int scale : new int[] {0, 19, 38, 75}) {
            if (scale < 1) {
                // is default for upload only as we custom encode BigDecimal by default
                new Decimal256RoundTripTest(BigDecimal.class, 1, scale).isDefaultUpload().runTest();
                new Decimal256RoundTripTest(BigInteger.class, 1, scale).runTest();
            }
            if (scale < 38) {
                new Decimal256RoundTripTest(BigDecimal.class, 38, scale).runTest();
                new Decimal256RoundTripTest(BigInteger.class, 38, scale).skipMapKey().runTest();
            }
            if (scale < 76) {
                new Decimal256RoundTripTest(BigDecimal.class, 76, scale).runTest();
                new Decimal256RoundTripTest(BigInteger.class, 76, scale).skipMapKey().runTest();
            }
        }

        // test dh coercion
        new Decimal256RoundTripTest(byte.class).runTest();
        new Decimal256RoundTripTest(char.class).runTest();
        new Decimal256RoundTripTest(short.class).runTest();
        new Decimal256RoundTripTest(int.class).runTest();
        new Decimal256RoundTripTest(long.class).runTest();

        final int floatDigits = (int) Math.floor(Math.log10(1L << 24));
        new DecimalRoundTripTest(float.class, floatDigits, floatDigits / 2).runTest();
        final int doubleDigits = (int) Math.floor(Math.log10(1L << 53));
        new DecimalRoundTripTest(double.class, doubleDigits, doubleDigits / 2).runTest();
    }

    @Test
    public void testBinary() throws Exception {
        new BinaryRoundTripTest(byte[].class).isDefault().skipMapKey().runTest();
        new BinaryRoundTripTest(ByteVector.class).skipMapKey().runTest();
        new BinaryRoundTripTest(ByteBuffer.class).skipMapKey().runTest();
    }

    @Test
    public void testFixedSizeBinary() throws Exception {
        new FixedSizeBinaryRoundTripTest(byte[].class, 16).isDefaultUpload().skipMapKey().runTest();
        new FixedSizeBinaryRoundTripTest(ByteVector.class, 12).skipMapKey().runTest();
        new FixedSizeBinaryRoundTripTest(ByteBuffer.class, 21).skipMapKey().runTest();
    }

    @Test
    public void testUtf8() throws Exception {
        new Utf8RoundTripTest(String.class, new ArrowType.Utf8()).isDefault().runTest();
    }

    @Test
    public void testFloatingPoint() throws Exception {
        for (FloatingPointPrecision precision : FloatingPointPrecision.values()) {
            new FloatingPointRoundTripTest(float.class, precision).checkIfDefault().runTest();
            new FloatingPointRoundTripTest(double.class, precision).runTest();
            new FloatingPointRoundTripTest(BigDecimal.class, precision).runTest();

            new FloatingPointRoundTripTest(byte.class, precision).runTest();
            new FloatingPointRoundTripTest(char.class, precision).runTest();
            new FloatingPointRoundTripTest(short.class, precision).runTest();
            new FloatingPointRoundTripTest(int.class, precision).runTest();
            new FloatingPointRoundTripTest(long.class, precision).runTest();
            new FloatingPointRoundTripTest(BigInteger.class, precision).runTest();
        }
    }

    @Test
    public void testTimestamp() throws Exception {
        new TimeStampRoundTripTest(Instant.class, TimeUnit.NANOSECOND).isDefault().runTest();
        new TimeStampRoundTripTest(Instant.class, TimeUnit.MICROSECOND).isDefaultUpload().runTest();
        new TimeStampRoundTripTest(Instant.class, TimeUnit.MILLISECOND).isDefaultUpload().runTest();
        new TimeStampRoundTripTest(Instant.class, TimeUnit.SECOND).isDefaultUpload().runTest();

        // note that default for upload only as the column source may lose the time zone
        new TimeStampRoundTripTest(ZonedDateTime.class, TimeUnit.NANOSECOND, "America/New_York").isDefaultUpload()
                .runTest();
        new TimeStampRoundTripTest(ZonedDateTime.class, TimeUnit.MICROSECOND, "America/New_York").runTest();
        new TimeStampRoundTripTest(ZonedDateTime.class, TimeUnit.MILLISECOND, "America/New_York").runTest();
        new TimeStampRoundTripTest(ZonedDateTime.class, TimeUnit.SECOND, "America/New_York").runTest();

        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.NANOSECOND).runTest();
        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.MICROSECOND).runTest();
        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.MILLISECOND).runTest();
        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.SECOND).runTest();

        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.NANOSECOND, "America/New_York").runTest();
        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.MICROSECOND, "America/New_York").runTest();
        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.MILLISECOND, "America/New_York").runTest();
        new TimeStampRoundTripTest(LocalDateTime.class, TimeUnit.SECOND, "America/New_York").runTest();
    }

    @Test
    public void testDuration() throws Exception {
        new DurationRoundTripTest(Duration.class, TimeUnit.NANOSECOND).isDefault().runTest();
        new DurationRoundTripTest(Duration.class, TimeUnit.MICROSECOND).isDefaultUpload().runTest();
        new DurationRoundTripTest(Duration.class, TimeUnit.MILLISECOND).isDefaultUpload().runTest();
        new DurationRoundTripTest(Duration.class, TimeUnit.SECOND).isDefaultUpload().runTest();
    }

    @Test
    public void testTime() throws Exception {
        new TimeRoundTripTest(LocalTime.class, TimeUnit.NANOSECOND).isDefault().runTest();
        new TimeRoundTripTest(LocalTime.class, TimeUnit.MICROSECOND).isDefaultUpload().runTest();
        new TimeRoundTripTest(LocalTime.class, TimeUnit.MILLISECOND).isDefaultUpload().runTest();
        new TimeRoundTripTest(LocalTime.class, TimeUnit.SECOND).isDefaultUpload().runTest();
    }

    @Test
    public void testDate() throws Exception {
        new DateRoundTripTest(LocalDate.class, DateUnit.DAY).isDefaultUpload().runTest();
        new DateRoundTripTest(LocalDate.class, DateUnit.MILLISECOND).isDefault().runTest();
    }

    @Test
    public void testInterval() throws Exception {
        new IntervalRoundTripTest(Duration.class, IntervalUnit.DAY_TIME).isDefaultUpload().runTest();
        new IntervalRoundTripTest(Period.class, IntervalUnit.YEAR_MONTH).isDefault().runTest();
        new IntervalRoundTripTest(PeriodDuration.class, IntervalUnit.MONTH_DAY_NANO).isDefault().runTest();

        // coercion mappings
        new IntervalRoundTripTest(Period.class, IntervalUnit.DAY_TIME).runTest();
        new IntervalRoundTripTest(Period.class, IntervalUnit.MONTH_DAY_NANO).runTest();
        new IntervalRoundTripTest(PeriodDuration.class, IntervalUnit.YEAR_MONTH).runTest();
        new IntervalRoundTripTest(PeriodDuration.class, IntervalUnit.DAY_TIME).runTest();
    }

    @Test
    public void testMultiUnion() throws Exception {
        final byte TYPE_ID_TIMESTAMP = 0;
        final byte TYPE_ID_STRING = 1;
        final byte TYPE_ID_LONG_LIST = 2;

        new RoundTripTest<DenseUnionVector>(Object.class) {
            // remember what we've assigned
            final int[] sourceTypeIds = new int[NUM_ROWS];
            final int[] sourceOffsets = new int[NUM_ROWS];

            @Override
            public void runTest() throws Exception {
                // TODO: revisit this test once https://github.com/apache/arrow-java/issues/399 is fixed
                // since arrow-java has significant bugs w.r.t. Lists of Unions
                runTest(TestWrapMode.NONE, TestNullMode.NONE);
                // for (TestWrapMode wrapMode : TestWrapMode.values()) {
                // if (wrapMode == TestWrapMode.FIXED_ARRAY || wrapMode == TestWrapMode.FIXED_VECTOR) {
                // // it appears that VectorSchemaRoot#create loses the inner dense union types; another bug
                // continue;
                // }
                // // note that dense union requires a separate null column if you want null values
                // runTest(wrapMode, TestNullMode.NONE);
                // }
            }

            @Override
            public Schema newSchema(boolean isNullable) {
                // Create child fields for the union
                Field timestampField = Field.nullable(
                        "timestampChild", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null));

                Field stringField = Field.nullable(
                        "stringChild", new ArrowType.Utf8());

                // For the list-of-long field, we must define a child item field:
                Field longItemField =
                        Field.nullable("longItem", new ArrowType.Int(64, /* signed */ true));
                Field longListField = new Field(
                        "longListChild", FieldType.nullable(new ArrowType.List()), List.of(longItemField));

                // Create a dense union type
                ArrowType.Union denseUnion = new ArrowType.Union(
                        UnionMode.Dense,
                        new int[] {TYPE_ID_TIMESTAMP, TYPE_ID_STRING, TYPE_ID_LONG_LIST});

                // Create the union field
                Field unionField = new Field(
                        COLUMN_NAME, FieldType.nullable(denseUnion),
                        List.of(timestampField, stringField, longListField));

                // Return a schema with just this field
                return new Schema(Collections.singletonList(unionField));
            }

            @Override
            public int initializeRoot(@NotNull final DenseUnionVector unionVec) {
                /*
                 * The DenseUnionVector has sub-vectors for each type ID. We retrieve them so we can store data directly
                 * into each sub-vector. For example:
                 *
                 * unionVec.getVectorByType(TYPE_ID_TIMESTAMP) -> TimeStampVector
                 * unionVec.getVectorByType(TYPE_ID_STRING) -> VarCharVector unionVec.getVectorByType(TYPE_ID_LONG_LIST)
                 * -> ListVector
                 */
                // Retrieve the child vectors for each type
                TimeStampVector tsChild = (TimeStampVector) unionVec.getVectorByType(TYPE_ID_TIMESTAMP);
                VarCharVector strChild = (VarCharVector) unionVec.getVectorByType(TYPE_ID_STRING);
                ListVector longList = (ListVector) unionVec.getVectorByType(TYPE_ID_LONG_LIST);

                // We'll keep track of how many values we've appended in each child vector
                int tsCount = 0;
                int strCount = 0;
                int listCount = 0;

                // Insert random data
                for (int i = 0; i < NUM_ROWS; i++) {
                    // Randomly pick one of the three union types
                    int pick = rnd.nextInt(3);

                    if (pick == 0) {
                        // 1) TIMESTAMP
                        unionVec.setTypeId(i, TYPE_ID_TIMESTAMP);
                        unionVec.setOffset(i, tsCount);

                        // Provide some random long for the timestamp (nanoseconds)
                        long randomNanos = rnd.nextLong();
                        // Set that into the child vector
                        tsChild.setSafe(tsCount, randomNanos);

                        sourceTypeIds[i] = TYPE_ID_TIMESTAMP;
                        sourceOffsets[i] = tsCount;
                        tsCount++;

                    } else if (pick == 1) {
                        // 2) STRING (UTF8)
                        unionVec.setTypeId(i, TYPE_ID_STRING);
                        unionVec.setOffset(i, strCount);

                        String randomString = getRandomUtf8String(rnd);
                        byte[] utf8Bytes = randomString.getBytes(StandardCharsets.UTF_8);
                        strChild.setSafe(strCount, utf8Bytes);

                        sourceTypeIds[i] = TYPE_ID_STRING;
                        sourceOffsets[i] = strCount;
                        strCount++;

                    } else {
                        // 3) LIST OF LONGS
                        unionVec.setTypeId(i, TYPE_ID_LONG_LIST);
                        unionVec.setOffset(i, listCount);

                        // Let's write 2 to 5 random longs per row in this example
                        int listSize = rnd.nextInt(4) + 2;
                        // Start the list
                        UnionListWriter listWriter = longList.getWriter();
                        listWriter.setPosition(listCount);
                        listWriter.startList();
                        for (int j = 0; j < listSize; j++) {
                            listWriter.bigInt().writeBigInt(rnd.nextLong());
                        }
                        listWriter.endList();

                        sourceTypeIds[i] = TYPE_ID_LONG_LIST;
                        sourceOffsets[i] = listCount;
                        listCount++;
                    }
                }

                // We need to set the final value counts
                tsChild.setValueCount(tsCount);
                strChild.setValueCount(strCount);
                longList.setValueCount(listCount);

                // The union itself must be set to NUM_ROWS
                unionVec.setValueCount(NUM_ROWS);

                return NUM_ROWS;
            }

            @Override
            public void validate(TestNullMode nullMode, @NotNull DenseUnionVector sourceVec,
                    @NotNull DenseUnionVector destVec) {
                // Retrieve child vectors on source
                TimeStampVector sourceTsChild = (TimeStampVector) sourceVec.getVectorByType(TYPE_ID_TIMESTAMP);
                VarCharVector sourceStrChild = (VarCharVector) sourceVec.getVectorByType(TYPE_ID_STRING);
                ListVector sourceList = (ListVector) sourceVec.getVectorByType(TYPE_ID_LONG_LIST);

                // Retrieve child vectors on dest
                TimeStampVector destTsChild = (TimeStampVector) destVec.getVectorByType(TYPE_ID_TIMESTAMP);
                VarCharVector destStrChild = (VarCharVector) destVec.getVectorByType(TYPE_ID_STRING);
                ListVector destList = (ListVector) destVec.getVectorByType(TYPE_ID_LONG_LIST);

                for (int i = 0; i < NUM_ROWS; i++) {
                    int expectedType = sourceTypeIds[i];
                    int actualType = destVec.getTypeId(i);
                    assertEquals(expectedType, actualType);

                    int expectedOffset = sourceOffsets[i];
                    int actualOffset = destVec.getOffset(i);
                    assertEquals(expectedOffset, actualOffset);

                    // Compare the actual data based on the type
                    switch (expectedType) {
                        case TYPE_ID_TIMESTAMP: {
                            long sourceVal = sourceTsChild.get(expectedOffset);
                            long destVal = destTsChild.get(actualOffset);
                            assertEquals(sourceVal, destVal);
                            break;
                        }
                        case TYPE_ID_STRING: {
                            byte[] sourceBytes = sourceStrChild.get(expectedOffset);
                            byte[] destBytes = destStrChild.get(actualOffset);
                            String sourceStr = new String(sourceBytes, StandardCharsets.UTF_8);
                            String destStr = new String(destBytes, StandardCharsets.UTF_8);
                            assertEquals(sourceStr, destStr);
                            break;
                        }
                        case TYPE_ID_LONG_LIST: {
                            // Compare the contents of the list
                            UnionListReader sourceListReader = sourceList.getReader();
                            UnionListReader destListReader = destList.getReader();

                            sourceListReader.setPosition(expectedOffset);
                            destListReader.setPosition(actualOffset);

                            // Build arrays of the child data
                            List<Long> sourceLongs = new ArrayList<>();
                            List<Long> destLongs = new ArrayList<>();

                            if (sourceListReader.isSet()) {
                                FieldReader sourceItemReader = sourceListReader.reader();
                                while (sourceListReader.next()) {
                                    sourceLongs.add(sourceItemReader.readLong());
                                }
                            }

                            if (destListReader.isSet()) {
                                FieldReader destItemReader = destListReader.reader();
                                while (destListReader.next()) {
                                    destLongs.add(destItemReader.readLong());
                                }
                            }

                            assertEquals(sourceLongs, destLongs);
                            break;
                        }
                        default:
                            throw new IllegalStateException("Unexpected type ID: " + expectedType);
                    }
                }
            }
        }.isDefaultUpload().runTest();
    }

    @Test
    public void testBinaryCustomMappings() throws Exception {
        new Utf8RoundTripTest(String.class, new ArrowType.Binary()).runTest();
        new CustomBinaryRoundTripTest(BigDecimal.class) {
            @Override
            public int initializeRoot(@NotNull final VarBinaryVector source) {
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    final BigDecimal bd = randomBigDecimal(rnd, 1 + rnd.nextInt(100), rnd.nextInt(100))
                            .stripTrailingZeros();
                    final int v = bd.scale();
                    byte[] biBytes = bd.unscaledValue().toByteArray();
                    byte[] valBytes = new byte[4 + biBytes.length];
                    valBytes[0] = (byte) (0xFF & v);
                    valBytes[1] = (byte) (0xFF & (v >> 8));
                    valBytes[2] = (byte) (0xFF & (v >> 16));
                    valBytes[3] = (byte) (0xFF & (v >> 24));
                    System.arraycopy(biBytes, 0, valBytes, 4, biBytes.length);
                    source.set(ii, valBytes);
                }

                return NUM_ROWS;
            }
        }.skipMapKey().runTest();

        new CustomBinaryRoundTripTest(BigInteger.class) {
            @Override
            public Object truncate(@NotNull final VarBinaryVector source, final int ii) {
                // to ensure that we don't have duplicate key values, we need to coerce back to BigInteger
                return new BigInteger(source.get(ii));
            }

            @Override
            public int initializeRoot(@NotNull final VarBinaryVector source) {
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    final BigInteger bi = new BigInteger(rnd.nextInt(100), rnd);
                    source.set(ii, bi.toByteArray());
                }

                return NUM_ROWS;
            }
        }.runTest();

        new CustomBinaryRoundTripTest(Schema.class) {

            @Override
            public int initializeRoot(@NotNull final VarBinaryVector source) {
                source.reallocDataBuffer(NUM_ROWS * 256);
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    final Schema schema = createSchema(rnd.nextBoolean(), true, new ArrowType.Bool(), null);
                    try (final ExposedByteArrayOutputStream os = new ExposedByteArrayOutputStream()) {
                        ArrowIpcUtil.serialize(os, schema);
                        source.set(ii, os.peekBuffer());
                    } catch (IOException e) {
                        throw new UncheckedDeephavenException(e);
                    }
                    source.set(ii, schema.serializeAsMessage());
                }

                return NUM_ROWS;
            }
        }.skipMapKey().runTest();
    }

    @Test
    public void testUnsupportedWireTypePropagation() throws Exception {
        try {
            new RoundTripTest<ViewVarCharVector>(String.class) {
                @Override
                public Schema newSchema(boolean isNullable) {
                    return createSchema(isNullable, isDefaultUpload, new ArrowType.Utf8View(), String.class);
                }
            }.runTest();
            Assert.statementNeverExecuted("Should have thrown an exception");
        } catch (FlightRuntimeException fre) {
            assertTrue(fre.getMessage().contains("No known Barrage ChunkReader"));
        }
    }

    @Test
    public void testUnsupportedMappingPropagation() throws Exception {
        try {
            new Utf8RoundTripTest(BarrageChunkFactoryTest.class, new ArrowType.Utf8()).runTest();
            Assert.statementNeverExecuted("Should have thrown an exception");
        } catch (FlightRuntimeException fre) {
            assertTrue(fre.getMessage().contains("No known Barrage ChunkReader"));
        }
    }

    @Test
    public void testNotAListDestinationPropagation() throws Exception {
        try {
            new RoundTripTest<>(Collection.class, Long.class) {
                @Override
                public Schema newSchema(boolean isNullable) {
                    final Map<String, String> attrs = new HashMap<>();
                    attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG,
                            Collection.class.getCanonicalName());
                    attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_COMPONENT_TYPE_TAG,
                            String.class.getCanonicalName());
                    final FieldType fieldType = new FieldType(isNullable, new ArrowType.List(), null, attrs);
                    final FieldType child = new FieldType(isNullable, new ArrowType.Int(64, true), null);
                    return new Schema(Collections.singletonList(new Field(COLUMN_NAME, fieldType,
                            List.of(new Field("COLUMN_NAME_CHILD", child, null)))));
                }
            }.runTest();
            Assert.statementNeverExecuted("Should have thrown an exception");
        } catch (FlightRuntimeException fre) {
            assertTrue(fre.getMessage().contains("No known Barrage ChunkReader"));
        }
    }

    @Test
    public void testDestClassNotFoundPropagation() throws Exception {
        try {
            new RoundTripTest<>(BarrageChunkFactoryTest.class) {
                @Override
                public Schema newSchema(boolean isNullable) {
                    final Map<String, String> attrs = new HashMap<>();
                    attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG,
                            "io.deephaven.test.NotARealClass");
                    final FieldType fieldType = new FieldType(isNullable, new ArrowType.Utf8(), null, attrs);
                    return new Schema(Collections.singletonList(new Field(COLUMN_NAME, fieldType, null)));
                }
            }.runTest();
            Assert.statementNeverExecuted("Should have thrown an exception");
        } catch (FlightRuntimeException fre) {
            assertTrue(fre.getMessage().contains("could not find class"));
        }
    }

    @Test
    public void testComponentDestClassNotFoundPropagation() throws Exception {
        try {
            new RoundTripTest<>(BarrageChunkFactoryTest.class) {
                @Override
                public Schema newSchema(boolean isNullable) {
                    final Map<String, String> attrs = new HashMap<>();
                    attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG, List.class.getCanonicalName());
                    attrs.put(BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_COMPONENT_TYPE_TAG,
                            "io.deephaven.test.NotARealClass");

                    final FieldType fieldType = new FieldType(isNullable, new ArrowType.Utf8(), null, attrs);
                    return new Schema(Collections.singletonList(new Field(COLUMN_NAME, fieldType, null)));
                }
            }.runTest();
            Assert.statementNeverExecuted("Should have thrown an exception");
        } catch (FlightRuntimeException fre) {
            assertTrue(fre.getMessage().contains("could not find class"));
        }
    }

    @SafeVarargs
    private static <T> int setAll(BiConsumer<Integer, T> setter, T... values) {
        for (int ii = 0; ii < values.length; ++ii) {
            setter.accept(ii, values[ii]);
        }
        return values.length;
    }

    protected enum TestRPCMethod {
        DO_GET, DO_EXCHANGE_NO_FILTER, HEAD, TAIL, COLUMNS_AS_LIST;

        boolean isPreviewEnabled() {
            // @formatter:off
            return switch (this) {
                case HEAD, TAIL -> true;
                default -> false;
            };
            // @formatter:on
        }

        private byte[] getOptionsMetadata(final boolean reading, final int ticket) {
            if (!reading && this == TestRPCMethod.COLUMNS_AS_LIST) {
                return BarrageUtil.createSerializationOptionsMetadataBytes(
                        flightTicketFor(ticket).getBytes(), getSubscriptionOptions());
            } else {
                return BarrageUtil.createSubscriptionRequestMetadataBytes(
                        flightTicketFor(ticket).getBytes(), getSubscriptionOptions());
            }
        }

        private BarrageSubscriptionOptions getSubscriptionOptions() {
            final BarrageSubscriptionOptions.Builder options = BarrageSubscriptionOptions.builder();
            if (this == TestRPCMethod.HEAD) {
                options.previewListLengthLimit(HEAD_LIST_ITEM_LEN);
            } else if (this == TestRPCMethod.TAIL) {
                options.previewListLengthLimit(-HEAD_LIST_ITEM_LEN);
            } else if (this == TestRPCMethod.COLUMNS_AS_LIST) {
                options.columnsAsList(true);
            }
            return options.build();
        }
    }
    protected enum TestNullMode {
        EMPTY, ALL, NONE, SOME, NOT_NULLABLE, NULL_WIRE
    }
    protected enum TestWrapMode {
        // @formatter:off
        NONE,
        FIXED_ARRAY,
        VAR_ARRAY,
        VIEW_ARRAY,
        FIXED_VECTOR,
        VAR_VECTOR,
        VIEW_VECTOR,
        UNION_DENSE,
        UNION_SPARSE,
        MAP_KEY,
        MAP_VALUE;
        // @formatter:on

        boolean isArrayLike() {
            switch (this) {
                case VAR_ARRAY:
                case VIEW_ARRAY:
                case VAR_VECTOR:
                case VIEW_VECTOR:
                    return true;
                default:
                    return false;
            }
        }

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

        boolean isFixedLength() {
            switch (this) {
                case FIXED_ARRAY:
                case FIXED_VECTOR:
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

        boolean isUnion() {
            switch (this) {
                case UNION_DENSE:
                case UNION_SPARSE:
                    return true;
                default:
                    return false;
            }
        }

        boolean isMap() {
            switch (this) {
                case MAP_KEY:
                case MAP_VALUE:
                    return true;
                default:
                    return false;
            }
        }

        boolean requiresSchema() {
            return isFixedLength() || isView() || isUnion() || isMap();
        }
    }

    private static ArrowType getWrappedModeType(final TestWrapMode mode, final Types.MinorType innerType) {
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
            case UNION_DENSE:
                return new ArrowType.Union(UnionMode.Dense, new int[] {0});
            case UNION_SPARSE:
                return new ArrowType.Union(UnionMode.Sparse, new int[] {innerType.ordinal()});
            case MAP_KEY:
            case MAP_VALUE:
                return new ArrowType.Map(false);
            default:
                throw new IllegalArgumentException("Unexpected array mode: " + mode);
        }
    }

    private abstract class RoundTripTest<T extends ValueVector> {
        protected final Random rnd = new Random(RANDOM_SEED);
        protected Class<?> dhType;
        protected Class<?> componentType;
        protected boolean isDefault;
        protected boolean isDefaultUpload;
        protected boolean skipMapKey;

        public RoundTripTest(@NotNull final Class<?> dhType) {
            this(dhType, null);
        }

        public RoundTripTest(@NotNull final Class<?> dhType, @Nullable final Class<?> componentType) {
            this.dhType = dhType;
            this.componentType = componentType;
        }

        public abstract Schema newSchema(boolean isNullable);

        public Object truncate(T source, int ii) {
            return source.getObject(ii);
        }

        public int initializeRoot(@NotNull T source) {
            // no-op default impl for some error cases
            return 0;
        }

        public void validate(TestNullMode nullMode, @NotNull T source, @NotNull T dest) {
            // no-op default impl for some error cases
        }

        public <RTT extends RoundTripTest<T>> RTT isDefault() {
            isDefault = true;
            isDefaultUpload = true;
            // noinspection unchecked
            return (RTT) this;
        }

        public <RTT extends RoundTripTest<T>> RTT isDefaultUpload() {
            isDefaultUpload = true;
            // noinspection unchecked
            return (RTT) this;
        }

        public <RTT extends RoundTripTest<T>> RTT skipMapKey() {
            skipMapKey = true;
            // noinspection unchecked
            return (RTT) this;
        }

        public void runTest() throws Exception {
            for (TestWrapMode wrapMode : TestWrapMode.values()) {
                for (TestNullMode nullMode : TestNullMode.values()) {
                    if (wrapMode == TestWrapMode.MAP_KEY) {
                        if (dhType == float.class || dhType == double.class || skipMapKey) {
                            // floating point values are terrible map keys
                            continue;
                        }
                        if (nullMode != TestNullMode.NOT_NULLABLE && nullMode != TestNullMode.EMPTY) {
                            // map keys can't be null
                            continue;
                        }
                    }
                    runTest(wrapMode, nullMode);
                }
            }

            if (dhType.isPrimitive()) {
                isDefault = false;
                isDefaultUpload = false;
                dhType = TypeUtils.getBoxedType(dhType);
                runBoxedTestsOnly();
            }
        }

        public void runBoxedTestsOnly() throws Exception {
            for (TestWrapMode wrapMode : TestWrapMode.values()) {
                if (wrapMode == TestWrapMode.NONE || wrapMode.isVector()) {
                    // can't have boxed vector's
                    continue;
                }
                for (TestNullMode nullMode : TestNullMode.values()) {
                    if (wrapMode == TestWrapMode.MAP_KEY) {
                        if (dhType == Float.class || dhType == Double.class) {
                            // floating point values are terrible map keys
                            continue;
                        }
                        if (nullMode != TestNullMode.NOT_NULLABLE && nullMode != TestNullMode.EMPTY) {
                            // map keys can't be null
                            continue;
                        }
                    }
                    runTest(wrapMode, nullMode);
                }
            }
        }

        public void runTest(
                final TestWrapMode wrapMode,
                final TestNullMode nullMode) throws Exception {
            if (!wrapMode.isArrayLike()) {
                runTest(wrapMode, nullMode, TestRPCMethod.DO_GET);
                return;
            }
            for (TestRPCMethod rpcMethod : TestRPCMethod.values()) {
                runTest(wrapMode, nullMode, rpcMethod);
            }
        }

        public void runTest(
                final TestWrapMode wrapMode,
                final TestNullMode nullMode,
                final TestRPCMethod rpcMethod) throws Exception {
            final boolean isNullable = nullMode != TestNullMode.NOT_NULLABLE;
            final boolean hasNulls = isNullable && nullMode != TestNullMode.NONE;
            final int listItemLength;
            final boolean resetIsDefault = isDefault && nullMode == TestNullMode.NULL_WIRE;
            final boolean resetIsDefaultUploadOnly = isDefaultUpload && nullMode == TestNullMode.NULL_WIRE;
            if (resetIsDefault) {
                isDefault = false;
            }
            if (resetIsDefaultUploadOnly) {
                isDefaultUpload = false;
            }
            Schema schema = newSchema(isNullable);
            if (resetIsDefault) {
                isDefault = true;
            }
            if (resetIsDefaultUploadOnly) {
                isDefaultUpload = true;
            }

            if (wrapMode == TestWrapMode.NONE) {
                listItemLength = 0;
            } else if (wrapMode.isUnion()) {
                listItemLength = 0;

                final Field innerField = schema.getFields().get(0);
                final Map<String, String> attrs = new LinkedHashMap<>(innerField.getMetadata());
                attrs.remove(DH_TYPE_TAG);
                attrs.remove(DH_COMPONENT_TYPE_TAG);

                final ArrowType unionType = getWrappedModeType(wrapMode,
                        Types.getMinorTypeForArrowType(innerField.getType()));
                final FieldType fieldType = new FieldType(isNullable, unionType, null, attrs);
                schema = new Schema(Collections.singletonList(
                        new Field(COLUMN_NAME, fieldType, Collections.singletonList(innerField))));
            } else if (wrapMode.isMap()) {
                listItemLength = 0;

                final Field innerField = schema.getFields().get(0);
                final Map<String, String> attrs = new LinkedHashMap<>(innerField.getMetadata());
                attrs.remove(DH_TYPE_TAG);
                attrs.remove(DH_COMPONENT_TYPE_TAG);

                final ArrowType mapType = new ArrowType.Map(false);

                final Field keyField;
                final Field valueField;
                if (wrapMode == TestWrapMode.MAP_KEY) {
                    // note map keys are not allowed to be null
                    keyField = new Field("key",
                            new FieldType(false, innerField.getType(), innerField.getDictionary(),
                                    innerField.getMetadata()),
                            Collections.emptyList());
                    valueField = new Field("value",
                            new FieldType(true, new ArrowType.Int(64, true), null, null),
                            Collections.emptyList());
                } else if (wrapMode == TestWrapMode.MAP_VALUE) {
                    keyField = new Field("key",
                            new FieldType(false, new ArrowType.Int(64, true), null, null),
                            Collections.emptyList());
                    valueField = new Field("value", innerField.getFieldType(), Collections.emptyList());
                } else {
                    throw new IllegalArgumentException("Unsupported test wrap mode: " + wrapMode);
                }

                final Field entriesField = new Field("MAP_ENTRIES",
                        new FieldType(false, new ArrowType.Struct(), null, null),
                        Arrays.asList(keyField, valueField));

                final FieldType fieldType = new FieldType(isNullable, mapType, null, attrs);

                schema = new Schema(Collections.singletonList(
                        new Field(COLUMN_NAME, fieldType, Collections.singletonList(entriesField))));
            } else {
                final Field innerField = schema.getFields().get(0);

                final Map<String, String> attrs = new LinkedHashMap<>(innerField.getMetadata());
                final String dhExplicitType = innerField.getMetadata().get(DH_TYPE_TAG);
                if (wrapMode.isVector()) {
                    final Class<?> vectorType = VectorFactory.forElementType(dhType).vectorType();
                    attrs.put(DH_TYPE_TAG, vectorType.getCanonicalName());
                } else if (dhExplicitType != null) {
                    attrs.put(DH_TYPE_TAG, dhExplicitType + "[]");
                }
                if (dhExplicitType != null) {
                    attrs.put(DH_COMPONENT_TYPE_TAG, dhExplicitType);
                }

                final ArrowType listType = getWrappedModeType(wrapMode,
                        Types.getMinorTypeForArrowType(innerField.getType()));
                final FieldType fieldType = new FieldType(isNullable, listType, null, attrs);
                schema = new Schema(Collections.singletonList(
                        new Field(COLUMN_NAME, fieldType, Collections.singletonList(innerField))));

                if (listType.getTypeID() == ArrowType.FixedSizeList.TYPE_TYPE) {
                    listItemLength = ((ArrowType.FixedSizeList) listType).getListSize();
                } else {
                    listItemLength = 0;
                }
            }

            if (nullMode == TestNullMode.NULL_WIRE) {
                final Field innerField = schema.getFields().get(0);

                final Map<String, String> attrs = new LinkedHashMap<>(innerField.getMetadata());
                if (innerField.getType().getTypeID() == ArrowType.ArrowTypeID.Map) {
                    attrs.put(DH_TYPE_TAG, Map.class.getCanonicalName());
                }
                final ArrowType nullType = ArrowType.Null.INSTANCE;
                final FieldType fieldType = new FieldType(true, nullType, null, attrs);

                schema = new Schema(Collections.singletonList(new Field(COLUMN_NAME, fieldType, null)));
            }

            if (rpcMethod == TestRPCMethod.COLUMNS_AS_LIST) {
                // we always have a single column; wrap it in a list
                final Field field = schema.getFields().get(0);
                final ArrowType listType = new ArrowType.List();
                final FieldType fieldType = new FieldType(false, listType, null, field.getMetadata());
                schema = new Schema(Collections.singletonList(new Field(
                        field.getName(), fieldType, Collections.singletonList(field))));
            }

            try (final VectorSchemaRoot source = VectorSchemaRoot.create(schema, allocator)) {
                source.allocateNew();
                final FieldVector dataVector = getDataVector(wrapMode, nullMode, rpcMethod, source, listItemLength);

                if (nullMode == TestNullMode.EMPTY) {
                    setSourceRowCount(rpcMethod, source, 0);
                } else {
                    // pre-allocate buffers
                    setSourceRowCount(rpcMethod, source, NUM_ROWS);

                    int numRows;
                    if (nullMode != TestNullMode.NULL_WIRE) {
                        // noinspection unchecked
                        numRows = initializeRoot((T) dataVector);
                    } else {
                        numRows = NUM_ROWS;
                    }

                    if (nullMode == TestNullMode.ALL || nullMode == TestNullMode.NULL_WIRE) {
                        for (int ii = 0; ii < numRows; ++ii) {
                            dataVector.setNull(ii);
                        }
                    } else if (nullMode == TestNullMode.SOME) {
                        for (int ii = 0; ii < numRows; ++ii) {
                            if (rnd.nextBoolean()) {
                                dataVector.setNull(ii);
                            }
                        }
                    }

                    if (nullMode == TestNullMode.NULL_WIRE) {
                        // do nothing
                    } else if (wrapMode == TestWrapMode.UNION_DENSE) {
                        final DenseUnionVector unionVec = (DenseUnionVector) getWrapModeVector(rpcMethod, source);
                        for (int ii = 0; ii < numRows; ++ii) {
                            unionVec.setTypeId(ii, (byte) 0);
                            unionVec.setOffset(ii, ii);
                        }
                    } else if (wrapMode == TestWrapMode.UNION_SPARSE) {
                        final UnionVector unionVec = (UnionVector) getWrapModeVector(rpcMethod, source);
                        final Types.MinorType minorType = dataVector.getMinorType();
                        for (int ii = 0; ii < numRows; ++ii) {
                            unionVec.setType(ii, minorType);
                        }
                    } else if (wrapMode.isMap()) {
                        int itemsConsumed = 0;
                        final MapVector mapVector = (MapVector) getWrapModeVector(rpcMethod, source);
                        final StructVector entriesVector = (StructVector) mapVector.getDataVector();

                        for (int ii = 0; ii < numRows; ++ii) {
                            if (hasNulls && rnd.nextBoolean()) {
                                mapVector.setNull(ii);
                                continue;
                            } else if (rnd.nextInt(8) == 0) {
                                // gen an empty map on occasion
                                mapVector.startNewValue(ii);
                                mapVector.endValue(ii, 0);
                                continue;
                            }
                            int numEntries = Math.min(rnd.nextInt(MAX_LIST_ITEM_LEN), numRows - itemsConsumed);
                            mapVector.startNewValue(ii);

                            final BigIntVector valueVector = (BigIntVector) entriesVector.getChild(
                                    wrapMode == TestWrapMode.MAP_KEY ? "value" : "key");
                            final BaseValueVector keyVector = (BaseValueVector) entriesVector.getChild("key");

                            final Set<Object> keys = wrapMode == TestWrapMode.MAP_VALUE ? null : new HashSet<>();
                            for (int jj = 0; jj < numEntries; jj++) {
                                int entryIndex = itemsConsumed + jj;
                                // avoid duplicate keys
                                if (keys != null) {
                                    // noinspection unchecked
                                    final Object val = truncate((T) keyVector, entryIndex);
                                    if (!keys.add(val)) {
                                        numEntries = jj;
                                        break;
                                    }
                                }
                                valueVector.setSafe(entryIndex, entryIndex);
                                entriesVector.setIndexDefined(entryIndex);
                            }

                            mapVector.endValue(ii, numEntries);
                            mapVector.getObject(ii);
                            itemsConsumed += numEntries;
                        }

                        // After populating all rows, set the total count on the underlying entries vector.
                        entriesVector.setValueCount(itemsConsumed);
                    } else if (wrapMode != TestWrapMode.NONE) {
                        if (listItemLength != 0) {
                            int realRows = numRows / listItemLength;
                            dataVector.setValueCount(realRows * listItemLength);
                            for (int ii = 0; ii < realRows; ++ii) {
                                FixedSizeListVector listVector =
                                        (FixedSizeListVector) getWrapModeVector(rpcMethod, source);
                                if (hasNulls && rnd.nextBoolean()) {
                                    listVector.setNull(ii);
                                    // to simplify validation, set inner values to null
                                    for (int jj = 0; jj < listItemLength; ++jj) {
                                        listVector.getDataVector().setNull(ii * listItemLength + jj);
                                    }
                                } else {
                                    listVector.setNotNull(ii);
                                }
                            }
                            setSourceRowCount(rpcMethod, source, realRows);
                            numRows = realRows;
                        } else if (wrapMode.isVariableLength()) {
                            int itemsConsumed = 0;
                            final ListVector listVector = (ListVector) getWrapModeVector(rpcMethod, source);
                            for (int ii = 0; ii < numRows; ++ii) {
                                if (hasNulls && rnd.nextBoolean()) {
                                    listVector.setNull(ii);
                                    continue;
                                } else if (rnd.nextInt(8) == 0) {
                                    listVector.startNewValue(ii);
                                    listVector.endValue(ii, 0);
                                    continue;
                                }
                                int itemLen = Math.min(rnd.nextInt(MAX_LIST_ITEM_LEN), numRows - itemsConsumed);
                                listVector.startNewValue(ii);
                                listVector.endValue(ii, itemLen);
                                itemsConsumed += itemLen;
                            }
                            dataVector.setValueCount(itemsConsumed);
                        } else {
                            final ListViewVector listVector = (ListViewVector) getWrapModeVector(rpcMethod, source);
                            dataVector.setValueCount(numRows);
                            int maxItemWritten = 0;
                            for (int ii = 0; ii < numRows; ++ii) {
                                if (hasNulls && rnd.nextBoolean()) {
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

                    // finally set the row count after the list vectors have been set, or else inner vectors might
                    // be cleared
                    setSourceRowCount(rpcMethod, source, numRows);
                }

                final boolean doExch = rpcMethod != TestRPCMethod.DO_GET;
                int flightDescriptorTicketValue = nextTicket++;
                FlightDescriptor descriptor = FlightDescriptor.path("export", flightDescriptorTicketValue + "");
                FlightClient.ClientStreamListener putStream =
                        flightClient.startPut(descriptor, source, new AsyncPutListener());
                if (doExch) {
                    putStream.putMetadata(getArrowBufMetadata(rpcMethod, false, flightDescriptorTicketValue));
                }
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
                final int outerRowCount = rpcMethod != TestRPCMethod.COLUMNS_AS_LIST
                        ? source.getRowCount()
                        : getWrapModeVector(rpcMethod, source).getValueCount();
                assertEquals(outerRowCount, uploadedTable.size());
                assertEquals(1, uploadedTable.getColumnSourceMap().size());
                ColumnSource<Object> columnSource = uploadedTable.getColumnSource(COLUMN_NAME);
                assertNotNull(columnSource);
                if (wrapMode == TestWrapMode.NONE) {
                    assertEquals(dhType, columnSource.getType());
                    assertEquals(componentType, columnSource.getComponentType());
                } else if (wrapMode.isUnion()) {
                    // the test declares these all as generic Object
                    assertEquals(Object.class, columnSource.getType());
                } else if (wrapMode.isMap()) {
                    // the test declares these all as generic Map
                    assertEquals(Map.class, columnSource.getType());
                } else {
                    if (wrapMode.isVector()) {
                        assertTrue(io.deephaven.vector.Vector.class.isAssignableFrom(columnSource.getType()));
                    } else {
                        assertTrue(columnSource.getType().isArray());
                        assertEquals(dhType, columnSource.getType().getComponentType());
                    }
                    assertEquals(dhType, columnSource.getComponentType());
                }

                // remove the uploaded DoPut schema; we want to test that defaults go both ways
                if (isDefault && !wrapMode.requiresSchema() && (dhType != byte.class || !wrapMode.isArrayLike())) {
                    // note that byte[] gets special treatment in the data world; is VarBinary instead of VarList
                    flightDescriptorTicketValue = nextTicket++;
                    currentSession.newExport(flightDescriptorTicketValue).submit(
                            () -> uploadedTable.withoutAttributes(Collections.singletonList(
                                    BaseTable.BARRAGE_SCHEMA_ATTRIBUTE)));
                }

                try (FlightStream stream = doExch
                        ? null
                        : flightClient.getStream(flightTicketFor(flightDescriptorTicketValue));
                        FlightClient.ExchangeReaderWriter xStream = doExch
                                ? flightClient.doExchange(flightDescriptorFor(flightDescriptorTicketValue))
                                : null) {
                    final VectorSchemaRoot dest;
                    if (!doExch) {
                        dest = stream.getRoot();
                    } else {
                        xStream.getWriter()
                                .putMetadata(getArrowBufMetadata(rpcMethod, true, flightDescriptorTicketValue));
                        dest = xStream.getReader().getRoot();
                    }

                    int numPayloads = 0;
                    while (doExch ? xStream.getReader().next() : stream.next()) {
                        assertEquals(source.getRowCount(), dest.getRowCount());

                        if (nullMode == TestNullMode.NULL_WIRE) {
                            // do nothing
                        } else if (wrapMode == TestWrapMode.UNION_DENSE) {
                            validateDenseUnion(nullMode,
                                    (DenseUnionVector) getWrapModeVector(rpcMethod, source),
                                    (DenseUnionVector) getWrapModeVector(rpcMethod, dest));
                        } else if (wrapMode == TestWrapMode.UNION_SPARSE) {
                            validateSparseUnion(nullMode,
                                    (UnionVector) getWrapModeVector(rpcMethod, source),
                                    (UnionVector) getWrapModeVector(rpcMethod, dest));
                        } else if (wrapMode != TestWrapMode.NONE) {
                            validateList(wrapMode, rpcMethod,
                                    (BaseListVector) getWrapModeVector(rpcMethod, source),
                                    (BaseListVector) getWrapModeVector(rpcMethod, dest));
                        }

                        if (nullMode == TestNullMode.NULL_WIRE) {
                            // no-op as long as we can round-trip the null wire type
                        } else if (wrapMode == TestWrapMode.NONE || wrapMode.isUnion()) {
                            // noinspection unchecked
                            validate(nullMode, (T) dataVector,
                                    (T) getDataVector(wrapMode, nullMode, rpcMethod, dest, listItemLength));
                        } else if (wrapMode.isView()) {
                            // TODO: rm this branch when https://github.com/apache/arrow-java/issues/471 is fixed

                            // DH will unwrap the view, so to validate the data vector we need to unwrap it as well
                            try (final ListViewVector newView =
                                    (ListViewVector) getWrapModeVector(rpcMethod, source).getField()
                                            .createVector(allocator)) {
                                newView.setValueCount(source.getRowCount());
                                final ListViewVector sourceArr = (ListViewVector) getWrapModeVector(rpcMethod, source);
                                int totalLen = 0;
                                for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                    if (!sourceArr.isNull(ii)) {
                                        // TODO: use when https://github.com/apache/arrow-java/issues/470 is fixed
                                        // totalLen += sourceArr.getElementEndIndex(ii) -
                                        // sourceArr.getElementStartIndex(ii);
                                        int size = sourceArr.getObject(ii).size();
                                        if (rpcMethod.isPreviewEnabled()) {
                                            size = Math.min(size, HEAD_LIST_ITEM_LEN);
                                        }
                                        totalLen += size;
                                    }
                                }
                                Assert.geqZero(totalLen, "totalLen");

                                newView.getDataVector().setValueCount(totalLen);
                                if (dhType == ZonedDateTime.class || dhType == LocalDateTime.class) {
                                    // TODO: remove branch when https://github.com/apache/arrow-java/issues/551 is fixed
                                    filterZonedDateTimeSource(wrapMode, rpcMethod, sourceArr, newView, source,
                                            listItemLength);
                                } else if ((dataVector instanceof DurationVector)
                                        && !(this instanceof IntervalRoundTripTest)) {
                                    // TODO: remove branch when https://github.com/apache/arrow-java/issues/558 is fixed
                                    filterDurationSource(wrapMode, rpcMethod, sourceArr, newView, source,
                                            listItemLength);
                                } else {
                                    for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                        if (sourceArr.isNull(ii)) {
                                            newView.setNull(ii);
                                        } else {
                                            copyListItem(rpcMethod, newView, sourceArr, ii);
                                        }
                                    }
                                }

                                // if the inner data is empty then we the inner DataVector will be a ZeroVector not a T
                                if (totalLen != 0) {
                                    FieldVector valueVectors = newView.getChildrenFromFields().get(0);
                                    if (wrapMode == TestWrapMode.MAP_KEY) {
                                        valueVectors = ((StructVector) valueVectors).getChild("key");
                                    } else if (wrapMode == TestWrapMode.MAP_VALUE) {
                                        valueVectors = ((StructVector) valueVectors).getChild("value");
                                    }

                                    // noinspection unchecked
                                    validate(nullMode, (T) valueVectors,
                                            (T) getDataVector(wrapMode, nullMode, rpcMethod, dest, listItemLength));
                                }
                            }
                        } else {
                            // any null values will not be sent back, so we need to filter the source to match
                            try (final BaseListVector newView =
                                    (BaseListVector) getWrapModeVector(rpcMethod, source).getField()
                                            .createVector(allocator)) {
                                newView.setValueCount(source.getRowCount());
                                final BaseListVector sourceArr = (BaseListVector) getWrapModeVector(rpcMethod, source);
                                int totalLen = 0;
                                for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                    if (!sourceArr.isNull(ii)) {
                                        int size =
                                                sourceArr.getElementEndIndex(ii) - sourceArr.getElementStartIndex(ii);
                                        if (rpcMethod.isPreviewEnabled()) {
                                            size = Math.min(size, HEAD_LIST_ITEM_LEN);
                                        }
                                        totalLen += size;
                                    }
                                }
                                Assert.geqZero(totalLen, "totalLen");

                                final int finTotalLen = totalLen;
                                newView.getChildrenFromFields().forEach(c -> c.setValueCount(finTotalLen));
                                if (dhType == ZonedDateTime.class || dhType == LocalDateTime.class) {
                                    // TODO: remove branch when https://github.com/apache/arrow-java/issues/551 is fixed
                                    filterZonedDateTimeSource(wrapMode, rpcMethod, sourceArr, newView, source,
                                            listItemLength);
                                } else if ((dataVector instanceof DurationVector)
                                        && !(this instanceof IntervalRoundTripTest)) {
                                    // TODO: remove branch when https://github.com/apache/arrow-java/issues/558 is fixed
                                    filterDurationSource(wrapMode, rpcMethod, sourceArr, newView, source,
                                            listItemLength);
                                } else {
                                    for (int ii = 0; ii < source.getRowCount(); ++ii) {
                                        if (sourceArr.isNull(ii)) {
                                            newView.setNull(ii);
                                        } else {
                                            // TODO: use when https://github.com/apache/arrow-java/issues/559 is fixed
                                            // newView.copyFrom(ii, ii, sourceArr);
                                            copyListItem(rpcMethod, newView, sourceArr, ii);
                                        }
                                    }
                                }

                                // if the inner data is empty then the inner DataVector will be a ZeroVector not a T
                                if (totalLen != 0) {
                                    FieldVector valueVectors = newView.getChildrenFromFields().get(0);
                                    if (wrapMode == TestWrapMode.MAP_KEY) {
                                        valueVectors = ((StructVector) valueVectors).getChild("key");
                                    } else if (wrapMode == TestWrapMode.MAP_VALUE) {
                                        valueVectors = ((StructVector) valueVectors).getChild("value");
                                    }

                                    // noinspection unchecked
                                    validate(nullMode, (T) valueVectors,
                                            (T) getDataVector(wrapMode, nullMode, rpcMethod, dest, listItemLength));
                                }
                            }
                        }
                        ++numPayloads;

                        if (doExch) {
                            // need to manually stop the subscription
                            xStream.getWriter().completed();
                            xStream.getReader().cancel("done", null);
                            break; // otherwise .next() will throw our cancellation exception
                        }
                    }

                    // if there is data, we should be able to encode in a single payload
                    assertEquals(!doExch && nullMode == TestNullMode.EMPTY ? 0 : 1, numPayloads);
                }
            }
        }
    }

    private static FieldVector getWrapModeVector(TestRPCMethod rpcMethod, VectorSchemaRoot source) {
        FieldVector retVal = source.getVector(0);
        if (rpcMethod == TestRPCMethod.COLUMNS_AS_LIST) {
            retVal = retVal.getChildrenFromFields().get(0);
        }
        return retVal;
    }

    private static void setSourceRowCount(TestRPCMethod rpcMethod, VectorSchemaRoot source, int numRows) {
        if (rpcMethod == TestRPCMethod.COLUMNS_AS_LIST) {
            source.setRowCount(1);
            for (final FieldVector wrappedField : source.getFieldVectors()) {
                List<FieldVector> children = wrappedField.getChildrenFromFields();
                Assert.eq(children.size(), "children.size()", 1);
                ((ListVector) wrappedField).startNewValue(0);
                ((ListVector) wrappedField).endValue(0, numRows);
            }
        } else {
            source.setRowCount(numRows);
        }
    }

    private @NotNull ArrowBuf getArrowBufMetadata(TestRPCMethod rpcMethod, boolean reading,
            int flightDescriptorTicketValue) {
        final byte[] request = rpcMethod.getOptionsMetadata(reading, flightDescriptorTicketValue);
        final ArrowBuf metadata = allocator.buffer(request.length);
        metadata.writeBytes(request);
        return metadata;
    }

    private static void filterZonedDateTimeSource(
            final TestWrapMode wrapMode,
            final TestRPCMethod previewMode,
            @NotNull final BaseListVector sourceArr,
            @NotNull final BaseListVector newView,
            @NotNull final VectorSchemaRoot source,
            final int listItemLength) {
        int newChildOffset = 0;
        final TimeStampVector srcChild;
        final TimeStampVector newChild;
        if (wrapMode.isMap()) {
            final String key = wrapMode == TestWrapMode.MAP_KEY ? "key" : "value";
            srcChild = (TimeStampVector) ((StructVector) sourceArr.getChildrenFromFields().get(0)).getChild(key);
            newChild = (TimeStampVector) ((StructVector) newView.getChildrenFromFields().get(0)).getChild(key);
        } else {
            srcChild = (TimeStampVector) sourceArr.getChildrenFromFields().get(0);
            newChild = (TimeStampVector) newView.getChildrenFromFields().get(0);
        }
        for (int ii = 0; ii < source.getRowCount(); ++ii) {
            if (sourceArr.isNull(ii)) {
                newView.setNull(ii);
                if (!wrapMode.isVariableLength()) {
                    newChildOffset += listItemLength;
                }
            } else {
                int srcStartOffset = sourceArr.getElementStartIndex(ii);
                // TODO: use when https://github.com/apache/arrow-java/issues/470 is fixed
                // int len = sourceArr.getElementEndIndex(ii) - srcStartOffset;
                int len = ((Collection<?>) sourceArr.getObject(ii)).size();
                if (previewMode.isPreviewEnabled()) {
                    if (previewMode == TestRPCMethod.TAIL && len > HEAD_LIST_ITEM_LEN) {
                        srcStartOffset += len - HEAD_LIST_ITEM_LEN;
                    }
                    len = Math.min(len, HEAD_LIST_ITEM_LEN);
                }
                if (listItemLength != 0) {
                    ((FixedSizeListVector) newView).setNotNull(ii);
                } else if (wrapMode.isVariableLength()) {
                    ListVector newAsLV = (ListVector) newView;
                    newAsLV.startNewValue(ii);
                    newAsLV.endValue(ii, len);
                }
                for (int jj = 0; jj < len; ++jj) {
                    final int so = srcStartOffset + jj;
                    if (srcChild.isNull(so)) {
                        newChild.setNull(newChildOffset + jj);
                    } else {
                        newChild.set(newChildOffset + jj, srcChild.get(so));
                    }
                }
                newChildOffset += len;
            }
        }
    }

    private static void filterDurationSource(
            final TestWrapMode wrapMode,
            final TestRPCMethod previewMode,
            @NotNull final BaseListVector sourceArr,
            @NotNull final BaseListVector newView,
            @NotNull final VectorSchemaRoot source,
            final int listItemLength) {
        int newChildOffset = 0;
        final DurationVector srcChild;
        final DurationVector newChild;
        if (wrapMode.isMap()) {
            final String key = wrapMode == TestWrapMode.MAP_KEY ? "key" : "value";
            srcChild = (DurationVector) ((StructVector) sourceArr.getChildrenFromFields().get(0)).getChild(key);
            newChild = (DurationVector) ((StructVector) newView.getChildrenFromFields().get(0)).getChild(key);
        } else {
            srcChild = (DurationVector) sourceArr.getChildrenFromFields().get(0);
            newChild = (DurationVector) newView.getChildrenFromFields().get(0);
        }
        for (int ii = 0; ii < source.getRowCount(); ++ii) {
            if (sourceArr.isNull(ii)) {
                newView.setNull(ii);
                if (!wrapMode.isVariableLength()) {
                    newChildOffset += listItemLength;
                }
            } else {
                int srcStartOffset = sourceArr.getElementStartIndex(ii);
                // TODO: use when https://github.com/apache/arrow-java/issues/470 is fixed
                // int len = sourceArr.getElementEndIndex(ii) - srcStartOffset;
                int len = ((Collection<?>) sourceArr.getObject(ii)).size();
                if (previewMode.isPreviewEnabled()) {
                    if (previewMode == TestRPCMethod.TAIL && len > HEAD_LIST_ITEM_LEN) {
                        srcStartOffset += len - HEAD_LIST_ITEM_LEN;
                    }
                    len = Math.min(len, HEAD_LIST_ITEM_LEN);
                }
                if (listItemLength != 0) {
                    ((FixedSizeListVector) newView).setNotNull(ii);
                } else if (wrapMode.isVariableLength()) {
                    ListVector newAsLV = (ListVector) newView;
                    newAsLV.startNewValue(ii);
                    newAsLV.endValue(ii, len);
                }
                for (int jj = 0; jj < len; ++jj) {
                    final int so = srcStartOffset + jj;
                    if (srcChild.isNull(so)) {
                        newChild.setNull(newChildOffset + jj);
                    } else {
                        final NullableDurationHolder h = new NullableDurationHolder();
                        srcChild.get(so, h);
                        newChild.set(newChildOffset + jj, h.value);
                    }
                }
                newChildOffset += len;
            }
        }
    }

    private void copyListItem(
            final TestRPCMethod previewMode,
            @NotNull final BaseListVector dest,
            @NotNull final BaseListVector source,
            final int index) {
        Preconditions.checkArgument(dest.getMinorType() == source.getMinorType());

        final FieldVector srcChildVector = source.getChildrenFromFields().get(0);
        if (srcChildVector instanceof FixedSizeBinaryVector) {
            // TODO: remove branch when https://github.com/apache/arrow-java/issues/559 is fixed
            final FixedSizeBinaryVector srcChild = (FixedSizeBinaryVector) srcChildVector;
            final FixedSizeBinaryVector dstChild = (FixedSizeBinaryVector) dest.getChildrenFromFields().get(0);
            int len = ((Collection<?>) source.getObject(index)).size();

            int srcOffset = source.getElementStartIndex(index);
            if (previewMode == TestRPCMethod.TAIL && len > HEAD_LIST_ITEM_LEN) {
                srcOffset += len - HEAD_LIST_ITEM_LEN;
            }
            if (previewMode.isPreviewEnabled()) {
                len = Math.min(len, HEAD_LIST_ITEM_LEN);
            }

            if (dest instanceof FixedSizeListVector) {
                ((FixedSizeListVector) dest).setNotNull(index);
            } else if (dest instanceof ListVector) {
                ((ListVector) dest).startNewValue(index);
                ((ListVector) dest).endValue(index, len);
            } else {
                ((ListViewVector) dest).startNewValue(index);
                ((ListViewVector) dest).endValue(index, len);
            }

            final int dstOffset = dest.getElementStartIndex(index);
            for (int jj = 0; jj < len; ++jj) {
                if (srcChild.isNull(srcOffset + jj)) {
                    dstChild.setNull(dstOffset + jj);
                } else {
                    dstChild.set(dstOffset + jj, srcChild.get(srcOffset + jj));
                }
            }
            return;
        }

        FieldReader in = source.getReader();
        in.setPosition(index);
        FieldWriter out = getListWriter(dest);
        out.setPosition(index);

        if (!in.isSet()) {
            out.writeNull();
            return;
        }

        out.startList();
        FieldReader childReader = in.reader();
        FieldWriter childWriter = getListWriterForReader(childReader, out);
        int endOffset = in.size();
        int startOffset = 0;
        if (previewMode == TestRPCMethod.TAIL && endOffset > HEAD_LIST_ITEM_LEN) {
            startOffset = endOffset - HEAD_LIST_ITEM_LEN;
        } else if (previewMode == TestRPCMethod.HEAD) {
            endOffset = Math.min(endOffset, HEAD_LIST_ITEM_LEN);
        }
        for (int ii = startOffset; ii < endOffset; ++ii) {
            childReader.setPosition(source.getElementStartIndex(index) + ii);
            if (!childReader.isSet()) {
                childWriter.writeNull();
                continue;
            }

            if (in.getMinorType() != Types.MinorType.MAP) {
                ComplexCopier.copy(childReader, childWriter);
                continue;
            }

            final List<Field> children = childReader.getField().getChildren();
            final Types.MinorType valueType = Types.getMinorTypeForArrowType(children.get(1).getType());
            if (childReader.getMinorType() != Types.MinorType.STRUCT
                    || valueType != Types.MinorType.FIXEDSIZEBINARY) {
                ComplexCopier.copy(childReader, childWriter);
                continue;
            }

            if (!childReader.isSet()) {
                childWriter.writeNull();
                continue;
            }

            childWriter.startEntry();

            // must ask for this struct before calling `key()`
            final FixedSizeBinaryWriter grandChildWriter = childWriter.struct().fixedSizeBinary("value");

            final FieldReader keyReader = childReader.reader("key");
            final FieldWriter keyWriter = getListWriterForReader(keyReader, childWriter.key());
            ComplexCopier.copy(keyReader, keyWriter);

            final FieldReader valReader = childReader.reader("value");
            if (!valReader.isSet()) {
                // TODO: rm grandchild, uncomment when https://github.com/apache/arrow-java/issues/586 is fixed
                // childWriter.values().fixedSizeBinary().writeNull();
                grandChildWriter.writeNull();
            } else {
                byte[] bytes = valReader.readByteArray();
                try (ArrowBuf buf = allocator.buffer(bytes.length)) {
                    // this is super annoying; but basically it's impossible to write a nullable fixed size
                    // binary without converting the child writer to a union!
                    buf.writeBytes(bytes);
                    // TODO: rm grandchild, uncomment when https://github.com/apache/arrow-java/issues/586 is fixed
                    // childWriter.value().fixedSizeBinary().writeFixedSizeBinary(buf);
                    grandChildWriter.writeFixedSizeBinary(buf);
                }
            }
            childWriter.endEntry();
            // we must advance the child writer or else endList will not include the final entry
            childWriter.setPosition(childWriter.getPosition() + 1);
        }
        // for some reason the childWriter and out share position, so we must reset the position so endList writes
        // the proper size
        out.setPosition(index);
        out.endList();
    }

    private static FieldWriter getListWriter(
            @NotNull final BaseListVector dest) {
        if (dest instanceof ListViewVector) {
            return ((ListViewVector) dest).getWriter();
        } else if (dest instanceof ListVector) {
            return ((ListVector) dest).getWriter();
        } else {
            return ((FixedSizeListVector) dest).getWriter();
        }
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
            case TIMESTAMPSECTZ:
                return (FieldWriter) writer.timeStampSecTZ();
            case TIMESTAMPMILLITZ:
                return (FieldWriter) writer.timeStampMilliTZ();
            case TIMESTAMPMICROTZ:
                return (FieldWriter) writer.timeStampMicroTZ();
            case TIMESTAMPNANOTZ:
                return (FieldWriter) writer.timeStampNanoTZ();
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
            case DURATION:
                return (FieldWriter) writer.duration();
            default:
                throw new UnsupportedOperationException(reader.getMinorType().toString());
        }
    }

    private static void validateDenseUnion(
            final TestNullMode nullMode,
            final DenseUnionVector source,
            final DenseUnionVector dest) {
        assertEquals(source.getValueCount(), dest.getValueCount());
        for (int ii = 0; ii < source.getValueCount(); ++ii) {
            if (source.isNull(ii)) {
                assertTrue(dest.isNull(ii));
                continue;
            }
            assertFalse(dest.isNull(ii));
            assertEquals(source.getTypeId(ii), dest.getTypeId(ii));
            assertEquals(source.getOffset(ii), dest.getOffset(ii));
        }
    }

    private static void validateSparseUnion(
            final TestNullMode nullMode,
            final UnionVector source,
            final UnionVector dest) {
        assertEquals(source.getValueCount(), dest.getValueCount());
        for (int ii = 0; ii < source.getValueCount(); ++ii) {
            if (source.isNull(ii)) {
                assertTrue(dest.isNull(ii));
                continue;
            }
            assertFalse(dest.isNull(ii));
            assertEquals(source.getTypeValue(ii), dest.getTypeValue(ii));
        }
    }

    private static void validateList(
            final TestWrapMode wrapMode,
            final TestRPCMethod previewMode,
            final BaseListVector source,
            final BaseListVector dest) {
        assertEquals(source.getValueCount(), dest.getValueCount());
        if (wrapMode.isVariableLength()) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    continue;
                }
                if (dest instanceof ListViewVector) {
                    // NOTE: https://github.com/apache/arrow-java/issues/470 points out that LVV's getElementEndIndex is
                    // returning size and not an offset.
                    assertEquals(source.getElementEndIndex(ii), dest.getElementEndIndex(ii));
                    continue;
                }
                int srcLen = source.getElementEndIndex(ii) - source.getElementStartIndex(ii);
                int dstLen = dest.getElementEndIndex(ii) - dest.getElementStartIndex(ii);
                if (previewMode.isPreviewEnabled()) {
                    srcLen = Math.min(srcLen, HEAD_LIST_ITEM_LEN);
                }
                assertEquals(srcLen, dstLen);
            }
        }
    }

    private static FieldVector getDataVector(
            final TestWrapMode wrapMode,
            final TestNullMode nullMode,
            final TestRPCMethod rpcMethod,
            final VectorSchemaRoot source,
            final int listItemLength) {

        FieldVector targetVector = getWrapModeVector(rpcMethod, source);

        if (wrapMode == TestWrapMode.NONE || nullMode == TestNullMode.NULL_WIRE) {
            return targetVector;
        } else if (wrapMode.isUnion()) {
            return ((AbstractContainerVector) targetVector).getChild(COLUMN_NAME);
        } else {
            if (listItemLength != 0) {
                final FixedSizeListVector arrayVector = (FixedSizeListVector) targetVector;
                return arrayVector.getDataVector();
            } else if (wrapMode.isVariableLength()) {
                final ListVector arrayVector = (ListVector) targetVector;
                return arrayVector.getDataVector();
            } else if (wrapMode == TestWrapMode.MAP_KEY) {
                final MapVector mapVector = (MapVector) targetVector;
                final StructVector arrayVector = (StructVector) mapVector.getDataVector();
                return arrayVector.getChild("key");
            } else if (wrapMode == TestWrapMode.MAP_VALUE) {
                final MapVector mapVector = (MapVector) targetVector;
                final StructVector arrayVector = (StructVector) mapVector.getDataVector();
                return arrayVector.getChild("value");
            } else {
                final ListViewVector arrayVector = (ListViewVector) targetVector;
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
        public Object truncate(final T source, int ii) {
            long truncated = getter.apply(source, ii).longValue();
            if (truncated != dhWireNull && truncate != null) {
                truncated = truncate.apply(truncated).longValue();
            }
            return truncated;
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

                long truncated = getter.apply(source, ii).longValue();
                if (truncated != dhWireNull) {
                    truncated = truncate.apply(truncated).longValue();
                }
                if (truncated == dhWireNull || truncated == dhSourceNull) {
                    if (nullMode == TestNullMode.NOT_NULLABLE) {
                        assertEquals(getter.apply(dest, ii).longValue(), dhSourceNull);
                    } else {
                        assertTrue(dest.isNull(ii));
                    }
                } else {
                    assertFalse(dest.isNull(ii));
                    long computed = getter.apply(dest, ii).longValue();
                    assertEquals(truncated, computed);
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
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Bool(), dhType);
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

        public DecimalRoundTripTest(@NotNull Class<?> dhType) {
            this(dhType, 0, 0);
        }

        public DecimalRoundTripTest(
                @NotNull Class<?> dhType, int precision, int scale) {
            super(dhType);

            if (dhType.isPrimitive() && !Set.of(float.class, double.class, boolean.class).contains(dhType)) {
                this.minValue = integralMin(dhType);
                this.maxValue = integralMax(dhType);
                this.precision = (int) Math.ceil(Math.log10(maxValue));
            } else {
                this.minValue = 0;
                this.maxValue = 0;
                this.precision = precision;
            }

            this.scale = scale;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Decimal(precision, scale, 128), dhType);
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
                } else if (dhType == BigInteger.class) {
                    final BigInteger srcVal = source.getObject(ii).toBigInteger();
                    final BigInteger dstVal = dest.getObject(ii).toBigInteger();
                    assertEquals(srcVal, dstVal);
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

        public Decimal256RoundTripTest(@NotNull Class<?> dhType) {
            this(dhType, 0, 0);
        }

        public Decimal256RoundTripTest(
                @NotNull Class<?> dhType, int precision, int scale) {
            super(dhType);

            if (dhType.isPrimitive() && !Set.of(float.class, double.class, boolean.class).contains(dhType)) {
                this.minValue = integralMin(dhType);
                this.maxValue = integralMax(dhType);
                this.precision = (int) Math.ceil(Math.log10(maxValue));
            } else {
                this.minValue = 0;
                this.maxValue = 0;
                this.precision = precision;
            }

            this.scale = scale;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Decimal(precision, scale, 256), dhType);
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
                } else if (dhType == BigInteger.class) {
                    final BigInteger srcVal = source.getObject(ii).toBigInteger();
                    final BigInteger dstVal = dest.getObject(ii).toBigInteger();
                    assertEquals(srcVal, dstVal);
                } else {
                    assertEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    // Float2Vector / Float4Vector / Float8Vector
    private class FloatingPointRoundTripTest extends RoundTripTest<FloatingPointVector> {
        final private FloatingPointPrecision fpp;

        final private long minValue;
        final private long maxValue;

        public FloatingPointRoundTripTest(
                @NotNull Class<?> dhType, FloatingPointPrecision fpp) {
            super(dhType);
            this.fpp = fpp;

            if ((dhType.isPrimitive() && !Set.of(float.class, double.class, boolean.class).contains(dhType))
                    || dhType == BigInteger.class) {
                // note that significands are limited to: 11 bits, 24 bits, and 53 bits (respectively for Float2/4/8)
                final int nbits;
                switch (fpp) {
                    case HALF:
                        nbits = 11;
                        break;
                    case SINGLE:
                        nbits = 24;
                        break;
                    case DOUBLE:
                        nbits = 53;
                        break;
                    default:
                        throw new IllegalArgumentException("Unexpected precision: " + fpp);
                }

                final long mask = (1L << (nbits + 1)) - 1;
                this.minValue = dhType == BigInteger.class ? -mask : Math.max(integralMin(dhType), -mask);
                this.maxValue = dhType == BigInteger.class ? mask : Math.min(integralMax(dhType), mask);
            } else {
                this.minValue = 0;
                this.maxValue = 0;
            }
        }

        public FloatingPointRoundTripTest checkIfDefault() {
            if (fpp == FloatingPointPrecision.HALF && dhType == float.class) {
                isDefaultUpload = true;
            } else if (fpp == FloatingPointPrecision.SINGLE && dhType == float.class) {
                isDefault = true;
                isDefaultUpload = true;
            } else if (fpp == FloatingPointPrecision.DOUBLE && dhType == double.class) {
                isDefault = true;
                isDefaultUpload = true;
            }
            return this;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.FloatingPoint(fpp), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final FloatingPointVector source) {
            if (source instanceof Float2Vector) {
                final Float2Vector f2v = (Float2Vector) source;
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    f2v.setWithPossibleTruncate(ii, nextFloat());
                    if (f2v.getValueAsDouble(ii) == QueryConstants.NULL_DOUBLE) {
                        --ii;
                    }
                }
            } else if (source instanceof Float4Vector) {
                final Float4Vector f4v = (Float4Vector) source;
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    f4v.setWithPossibleTruncate(ii, nextDouble());
                    if (f4v.getValueAsDouble(ii) == QueryConstants.NULL_DOUBLE) {
                        --ii;
                    }
                }
            } else if (source instanceof Float8Vector) {
                final Float8Vector f8v = (Float8Vector) source;
                for (int ii = 0; ii < NUM_ROWS; ++ii) {
                    f8v.setWithPossibleTruncate(ii, nextDouble());
                    if (f8v.getValueAsDouble(ii) == QueryConstants.NULL_DOUBLE) {
                        --ii;
                    }
                }
            } else {
                throw new IllegalArgumentException("Unexpected vector type: " + source.getClass());
            }
            return NUM_ROWS;
        }

        private BigInteger nextBigInt() {
            final BigInteger range = BigInteger.valueOf(maxValue).subtract(BigInteger.valueOf(minValue));
            return new BigInteger(range.bitLength(), rnd).mod(range).add(BigInteger.valueOf(minValue));
        }

        private float nextFloat() {
            if (maxValue != 0) {
                return nextBigInt().floatValue();
            }
            return rnd.nextFloat();
        }

        private double nextDouble() {
            if (maxValue != 0) {
                return nextBigInt().doubleValue();
            }
            return rnd.nextDouble();
        }

        @Override
        public void validate(
                final TestNullMode nullMode,
                @NotNull final FloatingPointVector source,
                @NotNull final FloatingPointVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    double srcValue = source.getValueAsDouble(ii);
                    assertEquals(srcValue, dest.getValueAsDouble(ii), srcValue * 1e-6);
                }
            }
        }
    }

    private class TimeStampRoundTripTest extends RoundTripTest<TimeStampVector> {
        private final TimeUnit timeUnit;
        private final String timeZone;

        public TimeStampRoundTripTest(
                @NotNull Class<?> dhType,
                final TimeUnit timeUnit) {
            this(dhType, timeUnit, null);
        }

        public TimeStampRoundTripTest(
                @NotNull Class<?> dhType,
                final TimeUnit timeUnit,
                @Nullable final String timeZone) {
            super(dhType);
            this.timeUnit = timeUnit;
            this.timeZone = timeZone;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Timestamp(timeUnit, timeZone), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final TimeStampVector source) {
            final long factor;
            if (timeUnit == TimeUnit.NANOSECOND) {
                factor = 1;
            } else if (timeUnit == TimeUnit.MICROSECOND) {
                factor = 1_000;
            } else if (timeUnit == TimeUnit.MILLISECOND) {
                factor = 1_000_000;
            } else if (timeUnit == TimeUnit.SECOND) {
                factor = 1_000_000_000;
            } else {
                throw new IllegalArgumentException("Unexpected time unit: " + timeUnit);
            }

            final ZoneId zid = timeZone == null ? null : ZoneId.of(timeZone);
            final ZoneId utc = ZoneId.of("UTC");
            for (int ii = 0; ii < NUM_ROWS; ++ii) {
                long epochNanos = rnd.nextLong();
                source.set(ii, epochNanos / factor);
                if (source.get(ii) == QueryConstants.NULL_LONG) {
                    --ii;
                }
                if (dhType == LocalDateTime.class && zid != null) {
                    final long epochSecs = epochNanos / 1_000_000_000L;
                    // ensure that this LDT is not affected by daylight savings time changes
                    ZonedDateTime lzdt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(epochSecs), zid);
                    // wash through LDT
                    lzdt = lzdt.toLocalDateTime().atZone(zid);
                    if (lzdt.withZoneSameInstant(utc).toEpochSecond() != epochSecs) {
                        --ii;
                    }
                }
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(TestNullMode nullMode, @NotNull TimeStampVector source, @NotNull TimeStampVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertEquals(source.get(ii), dest.get(ii));
                }
            }
        }
    }

    private class DurationRoundTripTest extends RoundTripTest<DurationVector> {
        private final TimeUnit timeUnit;

        public DurationRoundTripTest(
                @NotNull Class<?> dhType,
                final TimeUnit timeUnit) {
            super(dhType);
            this.timeUnit = timeUnit;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Duration(timeUnit), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final DurationVector source) {
            final long factor = factorForTimeUnit(timeUnit);
            for (int ii = 0; ii < NUM_ROWS; ++ii) {
                final long nextValue = rnd.nextLong() / factor;
                source.set(ii, nextValue);
                if (nextValue == QueryConstants.NULL_LONG) {
                    --ii;
                }
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(TestNullMode nullMode, @NotNull DurationVector source, @NotNull DurationVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private class TimeRoundTripTest extends RoundTripTest<BaseFixedWidthVector> {
        private final TimeUnit timeUnit;

        public TimeRoundTripTest(
                @NotNull Class<?> dhType,
                final TimeUnit timeUnit) {
            super(dhType);
            this.timeUnit = timeUnit;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            final int bw;
            if (timeUnit == TimeUnit.SECOND || timeUnit == TimeUnit.MILLISECOND) {
                bw = 32;
            } else {
                bw = 64;
            }
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Time(timeUnit, bw), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final BaseFixedWidthVector source) {
            final long factor;
            final BiConsumer<Integer, Long> setFunc;
            if (timeUnit == TimeUnit.NANOSECOND) {
                factor = 1;
                setFunc = (ii, val) -> ((TimeNanoVector) source).set(ii, val.longValue());
            } else if (timeUnit == TimeUnit.MICROSECOND) {
                factor = 1_000;
                setFunc = (ii, val) -> ((TimeMicroVector) source).set(ii, val.longValue());
            } else if (timeUnit == TimeUnit.MILLISECOND) {
                factor = 1_000_000;
                setFunc = (ii, val) -> ((TimeMilliVector) source).set(ii, val.intValue());
            } else if (timeUnit == TimeUnit.SECOND) {
                factor = 1_000_000_000;
                setFunc = (ii, val) -> ((TimeSecVector) source).set(ii, val.intValue());
            } else {
                throw new IllegalArgumentException("Unexpected time unit: " + timeUnit);
            }

            // this gets propagated to LocalTime#ofNanoOfDay; so we're limited to max nanos in a day
            final long nanosInDay = 24L * 60 * 60 * 1_000_000_000;
            for (int ii = 0; ii < NUM_ROWS; ++ii) {
                final long nextValue = (Math.abs(rnd.nextLong()) % nanosInDay) / factor;
                setFunc.accept(ii, nextValue);
                Assert.neq(nextValue, "nextValue", QueryConstants.NULL_LONG, "QueryConstants.NULL_LONG");
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(TestNullMode nullMode, @NotNull BaseFixedWidthVector source,
                @NotNull BaseFixedWidthVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private class DateRoundTripTest extends RoundTripTest<BaseFixedWidthVector> {
        private final DateUnit dateUnit;

        public DateRoundTripTest(
                @NotNull Class<?> dhType,
                final DateUnit dateUnit) {
            super(dhType);
            this.dateUnit = dateUnit;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Date(dateUnit), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final BaseFixedWidthVector source) {
            final long factor;
            final BiConsumer<Integer, Long> setFunc;
            if (dateUnit == DateUnit.DAY) {
                factor = 1;
                setFunc = (ii, val) -> ((DateDayVector) source).set(ii, val.intValue());
            } else if (dateUnit == DateUnit.MILLISECOND) {
                factor = 86_400_000;
                setFunc = (ii, val) -> ((DateMilliVector) source).set(ii, val * factor);
            } else {
                throw new IllegalArgumentException("Unexpected date unit: " + dateUnit);
            }

            for (int ii = 0; ii < NUM_ROWS; ++ii) {
                final long nextValue = rnd.nextLong() / factor;
                setFunc.accept(ii, nextValue);
                if (nextValue == QueryConstants.NULL_LONG) {
                    --ii;
                }
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(TestNullMode nullMode, @NotNull BaseFixedWidthVector source,
                @NotNull BaseFixedWidthVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private class IntervalRoundTripTest extends RoundTripTest<BaseFixedWidthVector> {
        private final IntervalUnit intervalUnit;

        public IntervalRoundTripTest(
                @NotNull Class<?> dhType,
                final IntervalUnit intervalUnit) {
            super(dhType);
            this.intervalUnit = intervalUnit;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Interval(intervalUnit), dhType);
        }

        @Override
        public int initializeRoot(@NotNull final BaseFixedWidthVector source) {
            // We'll populate random values depending on the interval type:
            // - YEAR_MONTH => single int (months)
            // - DAY_TIME => two ints (days, milliseconds)
            // - MONTH_DAY_NANO => (months, days, nanos)

            for (int ii = 0; ii < NUM_ROWS; ++ii) {
                switch (intervalUnit) {
                    case YEAR_MONTH: {
                        final IntervalYearVector iv = (IntervalYearVector) source;
                        final int months = rnd.nextInt(1 << 24);
                        iv.set(ii, months);
                        break;
                    }
                    case DAY_TIME: {
                        final IntervalDayVector iv = (IntervalDayVector) source;
                        // note that we can only represent ~100k days in a signed long of nanos
                        final int days = rnd.nextInt(100_000);
                        final int milliseconds = dhType == Period.class ? 0 : rnd.nextInt(24 * 60 * 60 * 1_000);
                        iv.set(ii, days, milliseconds);
                        break;
                    }
                    case MONTH_DAY_NANO: {
                        final IntervalMonthDayNanoVector iv = (IntervalMonthDayNanoVector) source;
                        final int months = rnd.nextInt(1 << 24);
                        final int days = rnd.nextInt(50_000);
                        final long nanos = Math.abs(rnd.nextLong()) % (24 * 60 * 60 * 1_000_000_000L);
                        iv.set(ii, months, days, dhType == Period.class ? 0 : nanos);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unexpected interval unit: " + intervalUnit);
                }
            }

            return NUM_ROWS;
        }

        @Override
        public void validate(TestNullMode nullMode, @NotNull BaseFixedWidthVector source,
                @NotNull BaseFixedWidthVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private class BinaryRoundTripTest extends RoundTripTest<VarBinaryVector> {

        public BinaryRoundTripTest(@NotNull Class<?> dhType) {
            super(dhType);
            if (dhType != ByteBuffer.class) {
                this.componentType = byte.class;
            }
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Binary(), dhType);
        }

        @Override
        public int initializeRoot(@NotNull VarBinaryVector source) {
            for (int i = 0; i < NUM_ROWS; i++) {
                int len = rnd.nextInt(16);
                byte[] data = new byte[len];
                rnd.nextBytes(data);
                source.setSafe(i, data);
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(
                final TestNullMode nullMode,
                @NotNull final VarBinaryVector source,
                @NotNull final VarBinaryVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ii++) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertArrayEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private class FixedSizeBinaryRoundTripTest extends RoundTripTest<FixedSizeBinaryVector> {
        final int fixedLength;

        public FixedSizeBinaryRoundTripTest(@NotNull Class<?> dhType, int fixedLength) {
            super(dhType);
            if (dhType != ByteBuffer.class) {
                this.componentType = byte.class;
            }
            this.fixedLength = fixedLength;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.FixedSizeBinary(fixedLength), dhType);
        }

        @Override
        public int initializeRoot(@NotNull FixedSizeBinaryVector source) {
            for (int i = 0; i < NUM_ROWS; i++) {
                byte[] data = new byte[fixedLength];
                rnd.nextBytes(data);
                source.setSafe(i, data);
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(
                final TestNullMode nullMode,
                @NotNull final FixedSizeBinaryVector source,
                @NotNull final FixedSizeBinaryVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ii++) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    assertArrayEquals(source.getObject(ii), dest.getObject(ii));
                }
            }
        }
    }

    private class Utf8RoundTripTest extends RoundTripTest<BaseVariableWidthVector> {
        private final ArrowType arrowType;

        public Utf8RoundTripTest(
                @NotNull final Class<?> dhType,
                final ArrowType arrowType) {
            super(dhType);
            this.arrowType = arrowType;
        }

        @Override
        public Object truncate(BaseVariableWidthVector source, int ii) {
            // override "truncate" to convert to String to guarantee unique keys when wrap mode is MAP_KEY
            Object innerObj = super.truncate(source, ii);
            if (innerObj instanceof byte[]) {
                return new String((byte[]) innerObj, Charsets.UTF_8);
            }
            return innerObj;
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, arrowType, dhType);
        }

        @Override
        public int initializeRoot(@NotNull final BaseVariableWidthVector source) {
            for (int ii = 0; ii < NUM_ROWS; ++ii) {
                String value = getRandomUtf8String(rnd);
                byte[] utf8Bytes = value.getBytes(StandardCharsets.UTF_8);
                source.setSafe(ii, utf8Bytes);
            }
            return NUM_ROWS;
        }

        @Override
        public void validate(TestNullMode nullMode, @NotNull BaseVariableWidthVector source,
                @NotNull BaseVariableWidthVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    final String sourceValue = new String(source.get(ii), StandardCharsets.UTF_8);
                    final String destValue = new String(dest.get(ii), StandardCharsets.UTF_8);
                    assertEquals(sourceValue, destValue);
                }
            }
        }
    }

    private static String getRandomUtf8String(final Random rnd) {
        int length = rnd.nextInt(20) + 1;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char ch = (char) (rnd.nextInt(26) + 'a');
            sb.append(ch);
        }
        return sb.toString();
    }

    private abstract class CustomBinaryRoundTripTest extends RoundTripTest<VarBinaryVector> {
        public CustomBinaryRoundTripTest(final @NotNull Class<?> dhType) {
            super(dhType);
        }

        @Override
        public Schema newSchema(boolean isNullable) {
            return createSchema(isNullable, isDefaultUpload, new ArrowType.Binary(), dhType);
        }

        @Override
        public void validate(TestNullMode nullMode, @NotNull VarBinaryVector source,
                @NotNull VarBinaryVector dest) {
            for (int ii = 0; ii < source.getValueCount(); ++ii) {
                if (source.isNull(ii)) {
                    assertTrue(dest.isNull(ii));
                } else {
                    try {
                        assertArrayEquals(source.getObject(ii), dest.getObject(ii));
                    } catch (Error e) {
                        throw e;
                    }
                }
            }
        }
    }

    private static Ticket flightTicketFor(int flightDescriptorTicketValue) {
        return new Ticket(FlightExportTicketHelper.exportIdToFlightTicket(flightDescriptorTicketValue).getTicket()
                .toByteArray());
    }

    private static FlightDescriptor flightDescriptorFor(int flightDescriptorTicketValue) {
        return FlightDescriptor.path("e", Integer.toString(flightDescriptorTicketValue));
    }

    private static long integralMin(final Class<?> dhType) {
        if (dhType == byte.class) {
            return QueryConstants.MIN_BYTE;
        } else if (dhType == char.class) {
            return QueryConstants.MIN_CHAR;
        } else if (dhType == short.class) {
            return QueryConstants.MIN_SHORT;
        } else if (dhType == int.class) {
            return QueryConstants.MIN_INT;
        } else if (dhType == long.class) {
            return QueryConstants.MIN_LONG;
        }
        throw new IllegalArgumentException("Unexpected type: " + dhType);
    }

    private static long integralMax(final Class<?> dhType) {
        if (dhType == byte.class) {
            return QueryConstants.MAX_BYTE;
        } else if (dhType == char.class) {
            return QueryConstants.MAX_CHAR;
        } else if (dhType == short.class) {
            return QueryConstants.MAX_SHORT;
        } else if (dhType == int.class) {
            return QueryConstants.MAX_INT;
        } else if (dhType == long.class) {
            return QueryConstants.MAX_LONG;
        }
        throw new IllegalArgumentException("Unexpected type: " + dhType);
    }
}
