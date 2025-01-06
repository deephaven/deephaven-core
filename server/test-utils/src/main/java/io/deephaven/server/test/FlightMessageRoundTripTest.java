//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.test;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.auth.codegen.impl.ConsoleServiceAuthWiring;
import io.deephaven.auth.codegen.impl.TableServiceContextualAuthWiring;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSnapshotOptions;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.ColumnConversionMode;
import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Assert;
import io.deephaven.client.impl.*;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.plugin.Registration;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.WrappedAuthenticationRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.proto.util.ScopeTicketHelper;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.config.ConfigServiceModule;
import io.deephaven.server.console.ConsoleModule;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.log.LogModule;
import io.deephaven.server.plugin.PluginsModule;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.server.runner.MainHelper;
import io.deephaven.server.session.*;
import io.deephaven.server.table.TableModule;
import io.deephaven.server.test.TestAuthModule.FakeBearer;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.IntVector;
import io.grpc.*;
import io.grpc.CallOptions;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.mutable.MutableBoolean;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

import static io.deephaven.client.impl.BarrageSubscriptionImpl.makeRequestInternal;
import static org.junit.Assert.*;

/**
 * Deliberately much lower in scope (and running time) than BarrageMessageRoundTripTest, the only purpose of this test
 * is to verify that we can round trip
 */
public abstract class FlightMessageRoundTripTest {
    private static final String ANONYMOUS = "Anonymous";
    private static final String DISABLED_FOR_TEST = "Disabled For Test";

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
    protected int localPort;
    private FlightClient flightClient;

    protected SessionService sessionService;

    private SessionState currentSession;
    private SafeCloseable executionContext;
    private Location serverLocation;
    protected TestComponent component;

    private ManagedChannel clientChannel;
    private ScheduledExecutorService clientScheduler;
    private Session clientSession;

    @BeforeClass
    public static void setupOnce() throws IOException {
        MainHelper.bootstrapProjectDirectories();
    }

    @Before
    public void setup() throws IOException, InterruptedException {
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
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(new RootAllocator()).intercept(info -> new FlightClientMiddleware() {
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

    protected abstract TestComponent component();

    @After
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

    @Test
    public void testLoginHandshakeBasicAuth() {
        closeClient();
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(new RootAllocator())
                .build();

        ExecutionContext.getContext().getQueryScope().putParam("test", TableTools.emptyTable(10).update("I=i"));

        // do get cannot be invoked by unauthenticated user
        final Ticket ticket = new Ticket("s/test".getBytes(StandardCharsets.UTF_8));
        fullyReadStream(ticket, true);

        // now login
        component.basicAuthHandler().validLogins.put("HANDSHAKE", "BASIC_AUTH");
        flightClient.authenticateBasic("HANDSHAKE", "BASIC_AUTH");

        // now we should see the scope variable
        fullyReadStream(ticket, false);
    }

    @Test
    public void testLoginHeaderBasicAuth() {
        closeClient();
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(new RootAllocator())
                .build();

        ExecutionContext.getContext().getQueryScope().putParam("test", TableTools.emptyTable(10).update("I=i"));

        // do get cannot be invoked by unauthenticated user
        final Ticket ticket = new Ticket("s/test".getBytes(StandardCharsets.UTF_8));
        fullyReadStream(ticket, true);

        // now login
        component.basicAuthHandler().validLogins.put("HANDSHAKE", "BASIC_AUTH");
        final Optional<CredentialCallOption> authOpt = flightClient.authenticateBasicToken("HANDSHAKE", "BASIC_AUTH");

        final CredentialCallOption auth = authOpt.orElseGet(() -> {
            throw Status.UNAUTHENTICATED.asRuntimeException();
        });

        // now we should see the scope variable
        fullyReadStream(ticket, false);
    }

    @Test
    public void testLoginHeaderCustomBearer() {
        closeClient();
        ExecutionContext.getContext().getQueryScope().putParam("test", TableTools.emptyTable(10).update("I=i"));

        // add the bearer token override
        final MutableBoolean tokenChanged = new MutableBoolean();
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(new RootAllocator())
                .intercept(info -> new FlightClientMiddleware() {
                    String currToken = Auth2Constants.BEARER_PREFIX + FakeBearer.TOKEN;

                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, currToken);
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {
                        String newToken = incomingHeaders.get(Auth2Constants.AUTHORIZATION_HEADER);
                        if (newToken != null) {
                            tokenChanged.setTrue();
                            currToken = newToken;
                        }
                    }

                    @Override
                    public void onCallCompleted(CallStatus status) {}
                }).build();

        // We will authenticate just prior to the do get request, so we should see the test table.
        final Ticket ticket = new Ticket("s/test".getBytes(StandardCharsets.UTF_8));
        fullyReadStream(ticket, false);
        Assert.eqTrue(tokenChanged.booleanValue(), "tokenChanged"); // assert we were sent a session token
    }

    @Test
    public void testLoginHandshakeAnonymous() {
        closeClient();
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(new RootAllocator())
                .build();

        ExecutionContext.getContext().getQueryScope().putParam("test", TableTools.emptyTable(10).update("I=i"));

        // do get cannot be invoked by unauthenticated user
        final Ticket ticket = new Ticket("s/test".getBytes(StandardCharsets.UTF_8));
        fullyReadStream(ticket, false);

        flightClient.authenticate(new ClientAuthHandler() {
            byte[] callToken = new byte[0];

            @Override
            public void authenticate(ClientAuthSender outgoing, Iterator<byte[]> incoming) {
                WrappedAuthenticationRequest request = WrappedAuthenticationRequest.newBuilder()
                        .setType(ANONYMOUS)
                        .setPayload(ByteString.EMPTY)
                        .build();
                outgoing.send(request.toByteArray());

                callToken = incoming.next();
            }

            @Override
            public byte[] getCallToken() {
                return callToken;
            }
        });

        // now we should see the scope variable
        fullyReadStream(ticket, false);
    }

    @Test
    public void testLoginHeaderAnonymous() {
        final String ANONYMOUS = "Anonymous";

        closeClient();
        ExecutionContext.getContext().getQueryScope().putParam("test", TableTools.emptyTable(10).update("I=i"));

        final MutableBoolean tokenChanged = new MutableBoolean();
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(new RootAllocator())
                .intercept(info -> new FlightClientMiddleware() {
                    String currToken = ANONYMOUS;

                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        outgoingHeaders.insert(Auth2Constants.AUTHORIZATION_HEADER, currToken);
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {
                        String newToken = incomingHeaders.get(Auth2Constants.AUTHORIZATION_HEADER);
                        if (newToken != null) {
                            tokenChanged.setTrue();
                            currToken = newToken;
                        }
                    }

                    @Override
                    public void onCallCompleted(CallStatus status) {}
                }).build();

        // We will authenticate just prior to the do get request, so we should see the test table.
        final Ticket ticket = new Ticket("s/test".getBytes(StandardCharsets.UTF_8));
        fullyReadStream(ticket, false);
        Assert.eqTrue(tokenChanged.booleanValue(), "tokenChanged"); // assert we were sent a session token
    }

    @Test
    public void testSimpleEmptyTableDoGet() throws Exception {
        Flight.Ticket simpleTableTicket = FlightExportTicketHelper.exportIdToFlightTicket(1);
        currentSession.newExport(simpleTableTicket, "test")
                .submit(() -> TableTools.emptyTable(10).update("I=i"));

        long totalRowCount = 0;
        try (FlightStream stream = flightClient.getStream(new Ticket(simpleTableTicket.getTicket().toByteArray()))) {
            while (stream.next()) {
                VectorSchemaRoot root = stream.getRoot();
                totalRowCount += root.getRowCount();

                // only one column was sent
                assertEquals(1, root.getFieldVectors().size());
                Field i = root.getSchema().findField("I");

                // all DH columns are nullable, even primitives
                assertTrue(i.getFieldType().isNullable());
                // verify it is a java int type, which is an arrow 32bit int
                assertEquals(ArrowType.ArrowTypeID.Int, i.getFieldType().getType().getTypeID());
                assertEquals(32, ((ArrowType.Int) i.getFieldType().getType()).getBitWidth());
                assertEquals("int", i.getMetadata().get("deephaven:type"));
            }

            // row count should match what we expect
            assertEquals(10, totalRowCount);
        }
    }

    @Test
    public void testRoundTripData() throws Exception {
        // tables without columns, as flight-based way of doing emptyTable
        assertRoundTripDataEqual(TableTools.emptyTable(0));
        assertRoundTripDataEqual(TableTools.emptyTable(10));

        // simple values, no nulls
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=String.valueOf(i)"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=(int)i"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=\"\""));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=0"));

        // non-flat RowSet
        assertRoundTripDataEqual(TableTools.emptyTable(10).where("i % 2 == 0").update("I=i"));

        // all null values in columns
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=(int)null"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=(String)null"));

        // some nulls in columns
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty= ((i % 2) == 0) ? i : (int)null"));
        assertRoundTripDataEqual(
                TableTools.emptyTable(10).update("empty= ((i % 2) == 0) ? String.valueOf(i) : (String)null"));

        // list columns
        assertRoundTripDataEqual(TableTools.emptyTable(5).update("A=i").groupBy().join(TableTools.emptyTable(5)));
    }

    @Test
    public void testTimestampColumns() throws Exception {
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("tm = DateTimeUtils.now()"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("instant = java.time.Instant.now()"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("zonedDateTime = java.time.ZonedDateTime.now()"));
    }

    @Test
    public void testStringCol() throws Exception {
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("S = \"test\""));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("S = new String[] {\"test\", \"42\"}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("S = new String[][] {new String[] {\"t1\"}}"));

        assertRoundTripDataEqual(TableTools.emptyTable(10).update("S = new String[][][] {" +
                "null, new String[][] {" +
                "   null, " +
                "   new String[] {" +
                "       null, \"elem_1_1_1\"" +
                "}}, new String[][] {" +
                "   null, " +
                "   new String[] {" +
                "       null, \"elem_2_1_1\"" +
                "   }, new String[] {" +
                "       null, \"elem_2_2_1\", \"elem_2_2_2\"" +
                "}}, new String[][] {" +
                "   null, " +
                "   new String[] {" +
                "       null, \"elem_3_1_1\"" +
                "   }, new String[] {" +
                "       null, \"elem_3_2_1\", \"elem_3_2_2\"" +
                "   }, new String[] {" +
                "       null, \"elem_3_3_1\", \"elem_3_3_2\", \"elem_3_3_3\"" +
                "}}}"));
    }

    @Test
    public void testLongCol() throws Exception {
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = ii"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = new long[] {ii}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = new long[][] {new long[] {ii}}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = (Long)ii"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = new Long[] {ii}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = new Long[] {ii, null}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = new Long[][] {new Long[] {ii}}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = new Long[][] {null, new Long[] {null, ii}}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("L = io.deephaven.util.QueryConstants.NULL_LONG"));
        assertRoundTripDataEqual(
                TableTools.emptyTable(10).update("L = new long[] {0, -1, io.deephaven.util.QueryConstants.NULL_LONG}"));
    }

    @Test
    public void testBoolCol() throws Exception {
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("B = true"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("B = new boolean[] {true, false}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10)
                .update("B = new boolean[][] {new boolean[] {false, true}}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("B = (Boolean)true"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("B = new Boolean[] {true, false}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10)
                .update("B = new Boolean[] {null, true, false}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10)
                .update("B = io.deephaven.util.QueryConstants.NULL_BOOLEAN"));
        assertRoundTripDataEqual(TableTools.emptyTable(10)
                .update("B = new Boolean[] {true, false, io.deephaven.util.QueryConstants.NULL_BOOLEAN}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10)
                .update("B = new Boolean[][] {new Boolean[] {false, true}}"));
        assertRoundTripDataEqual(TableTools.emptyTable(10)
                .update("B = new Boolean[][] {null, new Boolean[] {false, null, true}}"));
    }

    @Test
    public void testLocalDateCol() throws Exception {
        final Table source = TableTools.emptyTable(10).update("LD = java.time.LocalDate.ofEpochDay(ii)");
        final ColumnSource<LocalDate> ld = source.getColumnSource("LD");
        assertRoundTripDataEqual(source,
                recordBatch -> {
                    final FieldVector fv = recordBatch.getFieldVectors().get(0);
                    final DateMilliVector dmv = (DateMilliVector) fv;
                    for (int ii = 0; ii < source.intSize(); ++ii) {
                        Assert.equals(ld.get(ii), "ld.get(ii)", dmv.getObject(ii).toLocalDate(),
                                "dmv.getObject(ii).toLocalDate()");
                    }
                });
    }

    @Test
    public void testLocalTimeCol() throws Exception {
        final Table source = TableTools.emptyTable(10).update("LT = java.time.LocalTime.ofSecondOfDay(ii * 60 * 60)");
        final ColumnSource<LocalTime> lt = source.getColumnSource("LT");
        assertRoundTripDataEqual(source,
                recordBatch -> {
                    final FieldVector fv = recordBatch.getFieldVectors().get(0);
                    final TimeNanoVector tnv = (TimeNanoVector) fv;
                    for (int ii = 0; ii < source.intSize(); ++ii) {
                        Assert.eq(lt.get(ii).toNanoOfDay(), "lt.get(ii).toNanoOfDay()", tnv.get(ii), "tnv.get(ii)");
                    }
                });
    }

    @Test
    public void testFlightInfo() {
        final String staticTableName = "flightInfoTest";
        final String tickingTableName = "flightInfoTestTicking";
        final Table table = TableTools.emptyTable(10).update("I = i");

        final Table tickingTable = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                () -> TableTools.timeTable(1_000_000).update("I = i"));

        // stuff table into the scope
        ExecutionContext.getContext().getQueryScope().putParam(staticTableName, table);
        ExecutionContext.getContext().getQueryScope().putParam(tickingTableName, tickingTable);

        // test fetch info from scoped ticket
        assertInfoMatchesTable(flightClient.getInfo(arrowFlightDescriptorForName(staticTableName)), table);
        assertInfoMatchesTable(flightClient.getInfo(arrowFlightDescriptorForName(tickingTableName)), tickingTable);

        // test list flights which runs through scoped tickets
        final MutableInt seenTables = new MutableInt();
        flightClient.listFlights(Criteria.ALL).forEach(fi -> {
            seenTables.increment();
            if (fi.getDescriptor().equals(arrowFlightDescriptorForName(staticTableName))) {
                assertInfoMatchesTable(fi, table);
            } else {
                assertInfoMatchesTable(fi, tickingTable);
            }
        });

        Assert.eq(seenTables.get(), "seenTables.get()", 2);
    }

    @Test
    public void testGetSchema() {
        final String staticTableName = "flightInfoTest";
        final String tickingTableName = "flightInfoTestTicking";
        final Table table = TableTools.emptyTable(10).update("I = i");

        final Table tickingTable = ExecutionContext.getContext().getUpdateGraph().sharedLock().computeLocked(
                () -> TableTools.timeTable(1_000_000).update("I = i"));

        // stuff table into the scope
        ExecutionContext.getContext().getQueryScope().putParam(staticTableName, table);
        ExecutionContext.getContext().getQueryScope().putParam(tickingTableName, tickingTable);

        // test fetch info from scoped ticket
        assertSchemaMatchesTable(flightClient.getSchema(arrowFlightDescriptorForName(staticTableName)).getSchema(),
                table);
        assertSchemaMatchesTable(flightClient.getSchema(arrowFlightDescriptorForName(tickingTableName)).getSchema(),
                tickingTable);

        // test list flights which runs through scoped tickets
        final MutableInt seenTables = new MutableInt();
        flightClient.listFlights(Criteria.ALL).forEach(fi -> {
            seenTables.increment();
            if (fi.getDescriptor().equals(arrowFlightDescriptorForName(staticTableName))) {
                assertInfoMatchesTable(fi, table);
            } else {
                assertInfoMatchesTable(fi, tickingTable);
            }
        });

        Assert.eq(seenTables.get(), "seenTables.get()", 2);
    }

    @Test
    public void testDoExchangeSnapshot() throws Exception {
        final String staticTableName = "flightInfoTest";
        final Table table = TableTools.emptyTable(10).update("I = i", "J = i + 0.01");

        // stuff table into the scope
        ExecutionContext.getContext().getQueryScope().putParam(staticTableName, table);

        // build up a snapshot request
        byte[] magic = new byte[] {100, 112, 104, 110}; // equivalent to '0x6E687064' (ASCII "dphn")

        FlightDescriptor fd = FlightDescriptor.command(magic);

        try (FlightClient.ExchangeReaderWriter erw = flightClient.doExchange(fd);
                final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

            final FlatBufferBuilder metadata = new FlatBufferBuilder();

            // use 0 for batch size and max message size to use server-side defaults
            int optOffset = BarrageSnapshotOptions.createBarrageSnapshotOptions(metadata, false, 0, 0, 0);

            final int ticOffset =
                    BarrageSnapshotRequest.createTicketVector(metadata,
                            ScopeTicketHelper.nameToBytes(staticTableName));
            BarrageSnapshotRequest.startBarrageSnapshotRequest(metadata);
            BarrageSnapshotRequest.addColumns(metadata, 0);
            BarrageSnapshotRequest.addViewport(metadata, 0);
            BarrageSnapshotRequest.addSnapshotOptions(metadata, optOffset);
            BarrageSnapshotRequest.addTicket(metadata, ticOffset);
            metadata.finish(BarrageSnapshotRequest.endBarrageSnapshotRequest(metadata));

            final FlatBufferBuilder wrapper = new FlatBufferBuilder();
            final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
            wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                    wrapper,
                    0x6E687064, // the numerical representation of the ASCII "dphn".
                    BarrageMessageType.BarrageSnapshotRequest,
                    innerOffset));

            // extract the bytes and package them in an ArrowBuf for transmission
            byte[] msg = wrapper.sizedByteArray();
            ArrowBuf data = allocator.buffer(msg.length);
            data.writeBytes(msg);

            erw.getWriter().putMetadata(data);
            erw.getWriter().completed();

            // read everything from the server (expecting schema message and one data message)
            int totalRowCount = 0;
            while (erw.getReader().next()) {
                final int offset = totalRowCount;
                final VectorSchemaRoot root = erw.getReader().getRoot();
                final int rowCount = root.getRowCount();
                totalRowCount += rowCount;

                // check the values against the source table
                final org.apache.arrow.vector.IntVector iv = (org.apache.arrow.vector.IntVector) root.getVector(0);
                final IntVector sourceInts =
                        ColumnVectors.ofInt(table, table.getDefinition().getColumns().get(0).getName());
                for (int i = 0; i < rowCount; ++i) {
                    assertEquals("int match:", sourceInts.get(offset + i), iv.get(i));
                }
                final org.apache.arrow.vector.Float8Vector dv =
                        (org.apache.arrow.vector.Float8Vector) root.getVector(1);
                final DoubleVector sourceDoubles =
                        ColumnVectors.ofDouble(table, table.getDefinition().getColumns().get(1).getName());
                for (int i = 0; i < rowCount; ++i) {
                    assertEquals("double match: ", sourceDoubles.get(offset + i), dv.get(i), 0.000001);
                }
            }
            assertEquals(table.size(), totalRowCount);
        }
    }

    @Test
    public void testDoExchangeProtocol() throws Exception {
        final String staticTableName = "flightInfoTest";
        final Table table = TableTools.emptyTable(10).update("I = i", "J = i + 0.01");

        // stuff table into the scope
        ExecutionContext.getContext().getQueryScope().putParam(staticTableName, table);

        // java-flight requires us to send a message, but cannot add app metadata, send a dummy message
        byte[] empty = new byte[] {};
        final FlightDescriptor fd = FlightDescriptor.command(empty);
        try (FlightClient.ExchangeReaderWriter erw = flightClient.doExchange(fd);
                final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

            byte[] msg = new byte[0];
            ArrowBuf data = allocator.buffer(msg.length);
            data.writeBytes(msg);

            erw.getWriter().putMetadata(data);
            erw.getWriter().completed();

            Exception exception = assertThrows(FlightRuntimeException.class, () -> {
                erw.getReader().next();
            });

            String expectedMessage = "failed to receive Barrage request metadata";
            String actualMessage = exception.getMessage();

            assertTrue(actualMessage.contains(expectedMessage));
        }

    }

    @Test
    public void testAuthTicketTransformer() throws Exception {
        // stuff table into the scope
        final String tableName = "flightAuthTicketTransformTest";
        final String resultTableName = tableName + "Result";
        final Table table = TableTools.emptyTable(10).update("I = i", "J = i + 0.01");
        final MutableInt numTransforms = new MutableInt();
        component.authorizationProvider().delegateTicketTransformation = new NoopTicketResolverAuthorization() {
            @Override
            public <T> T transform(T source) {
                numTransforms.increment();
                if (source instanceof Table) {
                    // noinspection unchecked
                    return (T) ((Table) source).dropColumns("J");
                }
                return source;
            }
        };

        ExecutionContext.getContext().getQueryScope().putParam(tableName, table);

        // export from query scope to our session; this transforms the table
        assertEquals(0, numTransforms.get());
        try (final TableHandle handle = clientSession.execute(TicketTable.fromQueryScopeField(tableName))) {
            // place the transformed table into the scope; wait on the future to ensure the server-side operation
            // completes
            clientSession.publish(resultTableName, handle).get();
        }
        assertEquals(1, numTransforms.get());

        // check that the table was transformed
        Object result = ExecutionContext.getContext().getQueryScope().readParamValue(resultTableName, null);
        assertTrue(result + "", result instanceof Table);
        assertEquals(1, ((Table) result).getColumnSources().size());
        assertEquals(2, table.getColumnSources().size());
    }

    @Test
    public void testSimpleServiceAuthWiring() throws Exception {
        // stuff table into the scope
        final String tableName = "testSimpleServiceAuthWiringTest";
        final String resultTableName = tableName + "Result";
        final Table table = TableTools.emptyTable(10).update("I = -i", "J = -i");
        ExecutionContext.getContext().getQueryScope().putParam(tableName, table);

        // export from query scope to our session; this transforms the table
        try (final TableHandle handle = clientSession.execute(TicketTable.fromQueryScopeField(tableName))) {
            // verify that we can sort the table prior to the restriction
            clientSession.publish(resultTableName, handle).get();
            // verify that we can publish as many times as we please
            clientSession.publish(resultTableName, handle).get();

            component.authorizationProvider().getConsoleServiceAuthWiring().delegate =
                    new ConsoleServiceAuthWiring.AllowAll() {
                        @Override
                        public void onMessageReceivedBindTableToVariable(AuthContext authContext,
                                BindTableToVariableRequest request) {
                            ServiceAuthWiring.operationNotAllowed(DISABLED_FOR_TEST);
                        }
                    };

            try {
                clientSession.publish(resultTableName, handle).get();
                fail("expected the publish to fail");
            } catch (final Exception e) {
                // expect the authorization error details to propagate
                assertTrue(e.getMessage().contains(DISABLED_FOR_TEST));
            }
        }
    }

    @Test
    public void testSimpleContextualAuthWiring() throws Exception {
        // stuff table into the scope
        final String tableName = "testSimpleContextualAuthWiringTest";
        final Table table = TableTools.emptyTable(10).update("I = -i", "J = -i");
        ExecutionContext.getContext().getQueryScope().putParam(tableName, table);

        // export from query scope to our session; this transforms the table
        try (final TableHandle handle = clientSession.execute(TicketTable.fromQueryScopeField(tableName))) {

            // verify that we can sort the table prior to the restriction
            // noinspection EmptyTryBlock
            try (final TableHandle ignored = handle.sort("I")) {
                // ignore
            }

            component.authorizationProvider().getTableServiceContextualAuthWiring().delegate =
                    new TableServiceContextualAuthWiring.AllowAll() {
                        @Override
                        public void checkPermissionSort(AuthContext authContext, SortTableRequest request,
                                List<Table> sourceTables) {
                            ServiceAuthWiring.operationNotAllowed(DISABLED_FOR_TEST);
                        }
                    };

            try (final TableHandle ignored = handle.sort("J")) {
                fail("expected the sort to fail");
            } catch (final Exception e) {
                // expect the authorization error details to propagate
                assertTrue(e.getMessage().contains(DISABLED_FOR_TEST));
            }
        }
    }

    private static FlightDescriptor arrowFlightDescriptorForName(String name) {
        return FlightDescriptor.path(ScopeTicketHelper.nameToPath(name));
    }

    @Test
    public void testExportTicketVisibility() {
        // we have decided that if an api client creates export tickets, that they probably gain no value from
        // seeing them via Flight's listFlights but we do want them to work with getFlightInfo (or anywhere else a
        // flight ticket can be resolved).
        final Flight.Ticket ticket = FlightExportTicketHelper.exportIdToFlightTicket(1);
        final Table table = TableTools.emptyTable(10).update("I = i");
        currentSession.newExport(ticket, "test").submit(() -> table);

        // test fetch info from export ticket
        final FlightInfo info = flightClient.getInfo(FlightDescriptor.path("export", "1"));
        assertInfoMatchesTable(info, table);

        // test list flights which runs through scoped tickets
        flightClient.listFlights(Criteria.ALL).forEach(fi -> {
            throw new IllegalStateException("should not be included in list flights");
        });
    }

    private void assertInfoMatchesTable(FlightInfo info, Table table) {
        if (table.isRefreshing()) {
            Assert.eq(info.getRecords(), "info.getRecords()", -1);
        } else {
            Assert.eq(info.getRecords(), "info.getRecords()", table.size(), "table.size()");
        }
        // we don't try to compute this for the user; verify we are sending UNKNOWN instead of 0
        Assert.eq(info.getBytes(), "info.getBytes()", -1L);

        assertSchemaMatchesTable(info.getSchema(), table);
    }

    private void assertSchemaMatchesTable(Schema schema, Table table) {
        Assert.eq(schema.getFields().size(), "schema.getFields().size()", table.numColumns(),
                "table.numColumns()");
        Assert.equals(BarrageUtil.convertArrowSchema(schema).tableDef,
                "BarrageUtil.convertArrowSchema(schema)",
                table.getDefinition(), "table.getDefinition()");
    }

    private static int nextTicket = 1;


    private void assertRoundTripDataEqual(Table deephavenTable) throws Exception {
        assertRoundTripDataEqual(deephavenTable, recordBatch -> {
        });
    }

    private void assertRoundTripDataEqual(Table deephavenTable, Consumer<VectorSchemaRoot> recordBlockTester)
            throws Exception {
        // bind the table in the session
        Flight.Ticket dhTableTicket = FlightExportTicketHelper.exportIdToFlightTicket(nextTicket++);
        currentSession.newExport(dhTableTicket, "test").submit(() -> deephavenTable);

        // fetch with DoGet
        int flightDescriptorTicketValue;
        FlightClient.ClientStreamListener putStream;
        try (FlightStream stream = flightClient.getStream(new Ticket(dhTableTicket.getTicket().toByteArray()))) {
            VectorSchemaRoot root = stream.getRoot();

            // start the DoPut and send the schema
            flightDescriptorTicketValue = nextTicket++;
            FlightDescriptor descriptor = FlightDescriptor.path("export", flightDescriptorTicketValue + "");
            putStream = flightClient.startPut(descriptor, root, new AsyncPutListener());

            // send the body of the table
            while (stream.next()) {
                recordBlockTester.accept(root);
                putStream.putNext();
            }
        }

        // tell the server we are finished sending data
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

        // check that contents match
        assertEquals(deephavenTable.size(), uploadedTable.size());
        assertEquals(deephavenTable.getDefinition(), uploadedTable.getDefinition());
        assertEquals(0, (long) TableTools
                .diffPair(deephavenTable, uploadedTable, 0, EnumSet.noneOf(TableDiff.DiffItems.class)).getSecond());
    }

    @Test
    public void testColumnsAsListFeature() throws Exception {
        // bind the table in the session
        // this should be a refreshing table so we can validate that modifications are also wrapped
        final UpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph();
        try (final SafeCloseable ignored = updateGraph.sharedLock().lockCloseable()) {
            final Table appendOnly = TableTools.timeTable("PT1s")
                    .update("I = ii % 3", "J = `str_` + i");
            final Table withMods = appendOnly.lastBy("I");
            ExecutionContext.getContext().getQueryScope().putParam("test", withMods);
        }

        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.builder()
                .columnsAsList(true)
                .build();

        final TicketTable ticket = TicketTable.fromQueryScopeField("test");
        // fetch with DoExchange w/option enabled
        try (FlightClient.ExchangeReaderWriter stream = flightClient.doExchange(arrowFlightDescriptorForName("test"));
                final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

            // make a subscription request for test
            final ByteBuffer request = makeRequestInternal(null, null, false, options, ticket.ticket());
            ArrowBuf data = allocator.buffer(request.remaining());
            data.writeBytes(request.array(), request.arrayOffset() + request.position(), request.remaining());
            stream.getWriter().putMetadata(data);

            // read messages until we see at least one modification batch:
            for (int ii = 0; ii < 5; ++ii) {
                Assert.eqTrue(stream.getReader().next(), "stream.getReader().next()");
                final VectorSchemaRoot root = stream.getReader().getRoot();
                Assert.eqTrue(root.getVector("I") instanceof ListVector, "column is wrapped in list");
                Assert.eqTrue(root.getVector("J") instanceof ListVector, "column is wrapped in list");
                Assert.eqTrue(root.getVector("Timestamp") instanceof ListVector, "column is wrapped in list");
            }
        }
    }

    @Test
    public void testLongColumnWithFactor() {
        testLongColumnWithFactor(org.apache.arrow.vector.types.TimeUnit.SECOND, 1_000_000_000L);
        testLongColumnWithFactor(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 1_000_000L);
        testLongColumnWithFactor(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 1_000L);
        testLongColumnWithFactor(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 1L);
    }

    private void testLongColumnWithFactor(org.apache.arrow.vector.types.TimeUnit timeUnit, long factor) {
        final int exportId = nextTicket++;
        final Field field = Field.notNullable("Duration", new ArrowType.Duration(timeUnit));
        try (final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                final BigIntVector vector = new BigIntVector(field, allocator);
                final VectorSchemaRoot root = new VectorSchemaRoot(List.of(field), List.of(vector))) {
            final FlightClient.ClientStreamListener stream = flightClient.startPut(
                    FlightDescriptor.path("export", Integer.toString(exportId)), root, new SyncPutListener());

            final int numRows = 12;
            vector.allocateNew(numRows);
            for (int ii = 0; ii < numRows; ++ii) {
                vector.set(ii, ii % 3 == 0 ? QueryConstants.NULL_LONG : ii);
            }
            vector.setValueCount(numRows);

            root.setRowCount(numRows);
            stream.putNext();
            stream.completed();
            stream.getResult();

            final SessionState.ExportObject<Table> result = currentSession.getExport(exportId);
            Assert.eq(result.getState(), "result.getState()",
                    ExportNotification.State.EXPORTED, "ExportNotification.State.EXPORTED");
            Assert.eq(result.get().size(), "result.get().size()", numRows);
            final ColumnSource<Long> duration = result.get().getColumnSource("Duration");

            for (int ii = 0; ii < numRows; ++ii) {
                if (ii % 3 == 0) {
                    Assert.eq(duration.getLong(ii), "duration.getLong(ii)", QueryConstants.NULL_LONG,
                            "QueryConstants.NULL_LONG");
                } else {
                    Assert.eq(duration.getLong(ii), "duration.getLong(ii)", ii * factor, "ii * factor");
                }
            }
        }
    }

    @Test
    public void testInstantColumnWithFactor() {
        testInstantColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.SECOND, 1_000_000_000L, TimeStampSecVector::new);
        testInstantColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 1_000_000L, TimeStampMilliVector::new);
        testInstantColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 1_000L, TimeStampMicroVector::new);
        testInstantColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 1L, TimeStampNanoVector::new);
    }

    private interface TimeVectorFactory {
        TimeStampVector create(Field field, BufferAllocator allocator);
    }

    private void testInstantColumnWithFactor(
            org.apache.arrow.vector.types.TimeUnit timeUnit, long factor, TimeVectorFactory factory) {
        final int exportId = nextTicket++;
        final Field field = Field.notNullable("Time", new ArrowType.Timestamp(timeUnit, null));
        try (final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                final TimeStampVector vector = factory.create(field, allocator);
                final VectorSchemaRoot root = new VectorSchemaRoot(List.of(field), List.of(vector))) {
            final FlightClient.ClientStreamListener stream = flightClient.startPut(
                    FlightDescriptor.path("export", Integer.toString(exportId)), root, new SyncPutListener());

            final int numRows = 12;
            vector.allocateNew(numRows);
            for (int ii = 0; ii < numRows; ++ii) {
                vector.set(ii, ii % 3 == 0 ? QueryConstants.NULL_LONG : ii);
            }
            vector.setValueCount(numRows);

            root.setRowCount(numRows);
            stream.putNext();
            stream.completed();
            stream.getResult();

            final SessionState.ExportObject<Table> result = currentSession.getExport(exportId);
            Assert.eq(result.getState(), "result.getState()",
                    ExportNotification.State.EXPORTED, "ExportNotification.State.EXPORTED");
            Assert.eq(result.get().size(), "result.get().size()", numRows);
            final ColumnSource<Instant> time = result.get().getColumnSource("Time");

            for (int ii = 0; ii < numRows; ++ii) {
                if (ii % 3 == 0) {
                    Assert.eqNull(time.get(ii), "time.get(ii)");
                } else {
                    final long value = time.get(ii).getEpochSecond() * 1_000_000_000 + time.get(ii).getNano();
                    Assert.eq(value, "value", ii * factor, "ii * factor");
                }
            }
        }
    }

    @Test
    public void testZonedDateTimeColumnWithFactor() {
        testZonedDateTimeColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.SECOND, 1_000_000_000L, TimeStampSecVector::new);
        testZonedDateTimeColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 1_000_000L, TimeStampMilliVector::new);
        testZonedDateTimeColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 1_000L, TimeStampMicroVector::new);
        testZonedDateTimeColumnWithFactor(
                org.apache.arrow.vector.types.TimeUnit.NANOSECOND, 1L, TimeStampNanoVector::new);
    }

    private void testZonedDateTimeColumnWithFactor(
            org.apache.arrow.vector.types.TimeUnit timeUnit, long factor, TimeVectorFactory factory) {
        final int exportId = nextTicket++;
        final FieldType type = new FieldType(
                false, new ArrowType.Timestamp(timeUnit, null), null,
                Collections.singletonMap("deephaven:type", "java.time.ZonedDateTime"));
        final Field field = new Field("Time", type, null);
        try (final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                final TimeStampVector vector = factory.create(field, allocator);
                final VectorSchemaRoot root = new VectorSchemaRoot(List.of(field), List.of(vector))) {
            final FlightClient.ClientStreamListener stream = flightClient.startPut(
                    FlightDescriptor.path("export", Integer.toString(exportId)), root, new SyncPutListener());

            final int numRows = 12;
            vector.allocateNew(numRows);
            for (int ii = 0; ii < numRows; ++ii) {
                vector.set(ii, ii % 3 == 0 ? QueryConstants.NULL_LONG : ii);
            }
            vector.setValueCount(numRows);

            root.setRowCount(numRows);
            stream.putNext();
            stream.completed();
            stream.getResult();

            final SessionState.ExportObject<Table> result = currentSession.getExport(exportId);
            Assert.eq(result.getState(), "result.getState()",
                    ExportNotification.State.EXPORTED, "ExportNotification.State.EXPORTED");
            Assert.eq(result.get().size(), "result.get().size()", numRows);
            final ColumnSource<ZonedDateTime> time = result.get().getColumnSource("Time");

            for (int ii = 0; ii < numRows; ++ii) {
                if (ii % 3 == 0) {
                    Assert.eqNull(time.get(ii), "time.get(ii)");
                } else {
                    final long value = time.get(ii).toEpochSecond() * 1_000_000_000 + time.get(ii).getNano();
                    Assert.eq(value, "value", ii * factor, "ii * factor");
                }
            }
        }
    }

    private Schema createDoubleArraySchema() {
        final Field payload = new Field("", new FieldType(false, Types.MinorType.FLOAT8.getType(), null), null);

        final FieldType innerFieldType = new FieldType(true, Types.MinorType.LIST.getType(), null);
        final Field inner = new Field("", innerFieldType, Collections.singletonList(payload));

        final FieldType outerFieldType = new FieldType(true, Types.MinorType.LIST.getType(), null, Map.of(
                "deephaven:type", "double[][]"));
        final Field outer = new Field("data", outerFieldType, Collections.singletonList(inner));
        return new Schema(Collections.singletonList(outer));
    }

    @Test
    public void testNullNestedPrimitiveArray() {
        final int exportId = nextTicket++;

        try (final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                final VectorSchemaRoot root = VectorSchemaRoot.create(createDoubleArraySchema(), allocator)) {

            final ListVector outerVector = (ListVector) root.getVector(0);
            final FlightClient.ClientStreamListener stream = flightClient.startPut(
                    FlightDescriptor.path("export", Integer.toString(exportId)), root, new SyncPutListener());

            outerVector.allocateNew();
            UnionListWriter listWriter = new UnionListWriter(outerVector);

            final int numRows = 1;
            listWriter.writeNull();
            listWriter.setValueCount(numRows);
            root.setRowCount(numRows);

            stream.putNext();
            stream.completed();
            stream.getResult();

            final SessionState.ExportObject<Table> result = currentSession.getExport(exportId);
            Assert.eq(result.getState(), "result.getState()",
                    ExportNotification.State.EXPORTED, "ExportNotification.State.EXPORTED");
            Assert.eq(result.get().size(), "result.get().size()", numRows);
            final ColumnSource<?> data = result.get().getColumnSource("data");

            Assert.eqNull(data.get(0), "data.get(0)");
        }
    }

    @Test
    public void testEmptyNestedPrimitiveArray() {
        final int exportId = nextTicket++;

        try (final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                final VectorSchemaRoot root = VectorSchemaRoot.create(createDoubleArraySchema(), allocator)) {

            final ListVector outerVector = (ListVector) root.getVector(0);
            final FlightClient.ClientStreamListener stream = flightClient.startPut(
                    FlightDescriptor.path("export", Integer.toString(exportId)), root, new SyncPutListener());

            outerVector.allocateNew();
            UnionListWriter listWriter = new UnionListWriter(outerVector);

            final int numRows = 1;
            listWriter.startList();
            listWriter.endList();
            listWriter.setValueCount(numRows);

            root.setRowCount(numRows);

            stream.putNext();
            stream.completed();
            stream.getResult();

            final SessionState.ExportObject<Table> result = currentSession.getExport(exportId);
            Assert.eq(result.getState(), "result.getState()",
                    ExportNotification.State.EXPORTED, "ExportNotification.State.EXPORTED");
            Assert.eq(result.get().size(), "result.get().size()", numRows);
            final ColumnSource<?> data = result.get().getColumnSource("data");

            Assert.eqTrue(data.get(0) instanceof double[][], "data.get(0) instanceof double[][]");
            final double[][] arr = (double[][]) data.get(0);
            Assert.eq(arr.length, "arr.length", 0);
        }
    }

    @Test
    public void testInterestingNestedPrimitiveArray() {
        final int exportId = nextTicket++;

        try (final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
                final VectorSchemaRoot root = VectorSchemaRoot.create(createDoubleArraySchema(), allocator)) {

            final ListVector outerVector = (ListVector) root.getVector(0);
            final FlightClient.ClientStreamListener stream = flightClient.startPut(
                    FlightDescriptor.path("export", Integer.toString(exportId)), root, new SyncPutListener());

            outerVector.allocateNew();
            UnionListWriter listWriter = new UnionListWriter(outerVector);

            final int numRows = 1;
            // We want to recreate this structure:
            // new double[][] { null, new double[] {}, new double[] { 42.42f, 43.43f } }

            listWriter.startList();
            BaseWriter.ListWriter innerListWriter = listWriter.list();

            // null inner list
            innerListWriter.writeNull();

            // empty inner list
            innerListWriter.startList();
            innerListWriter.endList();

            // inner list with two values
            innerListWriter.startList();
            innerListWriter.float8().writeFloat8(42.42);
            innerListWriter.float8().writeFloat8(43.43);
            innerListWriter.endList();

            listWriter.endList();
            listWriter.setValueCount(numRows);
            root.setRowCount(numRows);

            stream.putNext();
            stream.completed();
            stream.getResult();

            final SessionState.ExportObject<Table> result = currentSession.getExport(exportId);
            Assert.eq(result.getState(), "result.getState()",
                    ExportNotification.State.EXPORTED, "ExportNotification.State.EXPORTED");
            Assert.eq(result.get().size(), "result.get().size()", numRows);
            final ColumnSource<?> data = result.get().getColumnSource("data");

            Assert.eqTrue(data.get(0) instanceof double[][], "data.get(0) instanceof double[][]");
            final double[][] arr = (double[][]) data.get(0);

            final int numInnerItems = 3;
            Assert.eq(arr.length, "arr.length", numInnerItems);

            for (int ii = 0; ii < numInnerItems; ++ii) {
                if (ii == 0) {
                    Assert.eqNull(arr[0], "arr[0]");
                } else {
                    Assert.neqNull(arr[ii], "arr[ii]");
                }
            }

            Assert.eq(arr[1].length, "arr[1].length", 0);

            Assert.eq(arr[2].length, "arr[2].length", 2);
            Assert.eq(arr[2][0], "arr[2][0]", 42.42);
            Assert.eq(arr[2][1], "arr[2][1]", 43.43);
        }
    }
}
