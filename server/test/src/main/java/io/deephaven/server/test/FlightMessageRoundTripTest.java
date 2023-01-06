/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.test;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.auth.AuthenticationRequestHandler;
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
import io.deephaven.client.impl.DaggerDeephavenFlightRoot;
import io.deephaven.client.impl.Export;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.FlightSessionFactory;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.io.logger.LogBuffer;
import io.deephaven.io.logger.LogBufferGlobal;
import io.deephaven.proto.backplane.grpc.SortTableRequest;
import io.deephaven.proto.backplane.grpc.WrappedAuthenticationRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.proto.util.ScopeTicketHelper;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionServiceGrpcImpl;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolver;
import io.deephaven.server.session.TicketResolverBase;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.auth.AuthContext;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth.ClientAuthHandler;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Deliberately much lower in scope (and running time) than BarrageMessageRoundTripTest, the only purpose of this test
 * is to verify that we can round trip
 */
public abstract class FlightMessageRoundTripTest {
    private static final String ANONYMOUS = "Anonymous";
    private static final String DISABLED_FOR_TEST = "Disabled For Test";

    @Module
    public static class FlightTestModule {
        @IntoSet
        @Provides
        TicketResolver ticketResolver(ScopeTicketResolver resolver) {
            return resolver;
        }

        @Singleton
        @Provides
        AbstractScriptSession<?> provideAbstractScriptSession() {
            return new NoLanguageDeephavenSession("non-script-session");
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
        static UpdateGraphProcessor provideUpdateGraphProcessor() {
            return UpdateGraphProcessor.DEFAULT;
        }
    }

    public interface TestComponent {
        Set<ServerInterceptor> interceptors();

        SessionServiceGrpcImpl sessionGrpcService();

        SessionService sessionService();

        AbstractScriptSession<?> scriptSession();

        GrpcServer server();

        TestAuthModule.BasicAuthTestImpl basicAuthHandler();

        Map<String, AuthenticationRequestHandler> authRequestHandlers();

        ExecutionContext executionContext();

        TestAuthorizationProvider authorizationProvider();
    }

    private LogBuffer logBuffer;
    private GrpcServer server;

    private FlightClient flightClient;

    protected SessionService sessionService;

    private SessionState currentSession;
    private AbstractScriptSession<?> scriptSession;
    private SafeCloseable executionContext;
    private Location serverLocation;
    private TestComponent component;

    private ManagedChannel clientChannel;
    private ScheduledExecutorService clientScheduler;
    private FlightSession clientSession;

    @Before
    public void setup() throws IOException {
        logBuffer = new LogBuffer(128);
        LogBufferGlobal.setInstance(logBuffer);

        component = component();

        server = component.server();
        server.start();
        int actualPort = server.getPort();

        scriptSession = component.scriptSession();
        sessionService = component.sessionService();
        executionContext = component.executionContext().open();

        serverLocation = Location.forGrpcInsecure("localhost", actualPort);
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

        clientChannel = ManagedChannelBuilder.forTarget("localhost:" + actualPort)
                .usePlaintext()
                .build();
        clientScheduler = Executors.newSingleThreadScheduledExecutor();
        FlightSessionFactory flightSessionFactory =
                DaggerDeephavenFlightRoot.create().factoryBuilder()
                        .managedChannel(clientChannel)
                        .scheduler(clientScheduler)
                        .allocator(new RootAllocator())
                        .build();

        clientSession = flightSessionFactory.newFlightSession();
    }

    protected abstract TestComponent component();

    @After
    public void teardown() throws InterruptedException {
        clientSession.close();
        clientScheduler.shutdownNow();
        clientChannel.shutdownNow();

        sessionService.closeAllSessions();
        scriptSession.release();
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

        scriptSession.setVariable("test", TableTools.emptyTable(10).update("I=i"));

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

        scriptSession.setVariable("test", TableTools.emptyTable(10).update("I=i"));

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
        scriptSession.setVariable("test", TableTools.emptyTable(10).update("I=i"));

        // add the bearer token override
        final String bearerToken = UUID.randomUUID().toString();
        component.authRequestHandlers().put(Auth2Constants.BEARER_PREFIX.trim(), new AuthenticationRequestHandler() {
            @Override
            public String getAuthType() {
                return Auth2Constants.BEARER_PREFIX.trim();
            }

            @Override
            public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload,
                    HandshakeResponseListener listener) {
                return Optional.empty();
            }

            @Override
            public Optional<AuthContext> login(String payload, MetadataResponseListener listener) {
                if (payload.equals(bearerToken)) {
                    return Optional.of(new AuthContext.SuperUser());
                }
                return Optional.empty();
            }

            @Override
            public void initialize(String targetUrl) {
                // do nothing
            }
        });

        final MutableBoolean tokenChanged = new MutableBoolean();
        flightClient = FlightClient.builder().location(serverLocation)
                .allocator(new RootAllocator())
                .intercept(info -> new FlightClientMiddleware() {
                    String currToken = Auth2Constants.BEARER_PREFIX + bearerToken;

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

        scriptSession.setVariable("test", TableTools.emptyTable(10).update("I=i"));

        // do get cannot be invoked by unauthenticated user
        final Ticket ticket = new Ticket("s/test".getBytes(StandardCharsets.UTF_8));
        fullyReadStream(ticket, false);

        // install the auth handler
        component.authRequestHandlers().put(ANONYMOUS, new AnonymousRequestHandler());

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
        scriptSession.setVariable("test", TableTools.emptyTable(10).update("I=i"));

        // install the auth handler
        component.authRequestHandlers().put(ANONYMOUS, new AnonymousRequestHandler());

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
    public void testTimestampColumn() throws Exception {
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("tm = DateTime.now()"));
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
    public void testFlightInfo() {
        final String staticTableName = "flightInfoTest";
        final String tickingTableName = "flightInfoTestTicking";
        final Table table = TableTools.emptyTable(10).update("I = i");

        final Table tickingTable = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> TableTools.timeTable(1_000_000).update("I = i"));

        // stuff table into the scope
        scriptSession.setVariable(staticTableName, table);
        scriptSession.setVariable(tickingTableName, tickingTable);

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

        Assert.eq(seenTables.intValue(), "seenTables.intValue()", 2);
    }

    @Test
    public void testGetSchema() {
        final String staticTableName = "flightInfoTest";
        final String tickingTableName = "flightInfoTestTicking";
        final Table table = TableTools.emptyTable(10).update("I = i");

        final Table tickingTable = UpdateGraphProcessor.DEFAULT.sharedLock()
                .computeLocked(() -> TableTools.timeTable(1_000_000).update("I = i"));

        try (final SafeCloseable ignored = LivenessScopeStack.open(scriptSession, false)) {
            // stuff table into the scope
            scriptSession.setVariable(staticTableName, table);
            scriptSession.setVariable(tickingTableName, tickingTable);

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

            Assert.eq(seenTables.intValue(), "seenTables.intValue()", 2);
        }
    }

    @Test
    public void testDoExchangeSnapshot() throws Exception {
        final String staticTableName = "flightInfoTest";
        final Table table = TableTools.emptyTable(10).update("I = i", "J = i + 0.01");

        try (final SafeCloseable ignored = LivenessScopeStack.open(scriptSession, false)) {
            // stuff table into the scope
            scriptSession.setVariable(staticTableName, table);

            // build up a snapshot request
            byte[] magic = new byte[] {100, 112, 104, 110}; // equivalent to '0x6E687064' (ASCII "dphn")

            FlightDescriptor fd = FlightDescriptor.command(magic);

            try (FlightClient.ExchangeReaderWriter erw = flightClient.doExchange(fd);
                    final RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {

                final FlatBufferBuilder metadata = new FlatBufferBuilder();

                // use 0 for batch size and max message size to use server-side defaults
                int optOffset =
                        BarrageSnapshotOptions.createBarrageSnapshotOptions(metadata, ColumnConversionMode.Stringify,
                                false, 0, 0);

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
                    org.apache.arrow.vector.IntVector iv =
                            (org.apache.arrow.vector.IntVector) root.getVector(0);
                    for (int i = 0; i < rowCount; ++i) {
                        assertEquals("int match:", table.getColumn(0).get(offset + i), iv.get(i));
                    }
                    org.apache.arrow.vector.Float8Vector dv =
                            (org.apache.arrow.vector.Float8Vector) root.getVector(1);
                    for (int i = 0; i < rowCount; ++i) {
                        assertEquals("double match: ", table.getColumn(1).get(offset + i), dv.get(i));
                    }
                }
                assertEquals(table.size(), totalRowCount);
            }
        }
    }

    @Test
    public void testDoExchangeProtocol() throws Exception {
        final String staticTableName = "flightInfoTest";
        final Table table = TableTools.emptyTable(10).update("I = i", "J = i + 0.01");

        try (final SafeCloseable ignored = LivenessScopeStack.open(scriptSession, false)) {
            // stuff table into the scope
            scriptSession.setVariable(staticTableName, table);

            // build up a snapshot request incorrectly
            byte[] empty = new byte[0];

            FlightDescriptor fd = FlightDescriptor.command(empty);

            try (FlightClient.ExchangeReaderWriter erw = flightClient.doExchange(fd)) {

                Exception exception = assertThrows(FlightRuntimeException.class, () -> {
                    erw.getReader().next();
                });

                String expectedMessage = "expected BarrageMessageWrapper magic bytes in FlightDescriptor.cmd";
                String actualMessage = exception.getMessage();

                assertTrue(actualMessage.contains(expectedMessage));
            }

            byte[] magic = new byte[] {100, 112, 104, 110}; // equivalent to '0x6E687064' (ASCII "dphn")
            fd = FlightDescriptor.command(magic);
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
    }

    @Test
    public void testAuthTicketTransformer() throws Exception {
        // stuff table into the scope
        final String tableName = "flightAuthTicketTransformTest";
        final String resultTableName = tableName + "Result";
        final Table table = TableTools.emptyTable(10).update("I = i", "J = i + 0.01");
        final MutableInt numTransforms = new MutableInt();
        component.authorizationProvider().delegateTicketTransformation = new TicketResolverBase.AuthTransformation() {
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

        scriptSession.setVariable(tableName, table);

        // export from query scope to our session; this transforms the table
        assertEquals(0, numTransforms.intValue());
        final Export export = clientSession.session().export(TicketTable.fromQueryScopeField(tableName));
        // place the transformed table into the scope; wait on the future to ensure the server-side operation completes
        clientSession.session().publish(resultTableName, export).get();
        assertEquals(1, numTransforms.intValue());

        // check that the table was transformed
        Object result = scriptSession.getVariable(resultTableName);
        assertTrue(result instanceof Table);
        assertEquals(1, ((Table) result).getColumnSources().size());
        assertEquals(2, table.getColumnSources().size());
    }

    @Test
    public void testSimpleServiceAuthWiring() throws Exception {
        // stuff table into the scope
        final String tableName = "testSimpleServiceAuthWiringTest";
        final String resultTableName = tableName + "Result";
        final Table table = TableTools.emptyTable(10).update("I = -i", "J = -i");
        scriptSession.setVariable(tableName, table);

        // export from query scope to our session; this transforms the table
        final Export export = clientSession.session().export(TicketTable.fromQueryScopeField(tableName));

        // verify that we can sort the table prior to the restriction
        clientSession.session().publish(resultTableName, export).get();
        // verify that we can publish as many times as we please
        clientSession.session().publish(resultTableName, export).get();

        component.authorizationProvider().getConsoleServiceAuthWiring().delegate =
                new ConsoleServiceAuthWiring.AllowAll() {
                    @Override
                    public void onMessageReceivedBindTableToVariable(AuthContext authContext,
                            BindTableToVariableRequest request) {
                        ServiceAuthWiring.operationNotAllowed(DISABLED_FOR_TEST);
                    }
                };

        try {
            clientSession.session().publish(resultTableName, export).get();
            fail("expected the publish to fail");
        } catch (final Exception e) {
            // expect the authorization error details to propagate
            assertTrue(e.getMessage().contains(DISABLED_FOR_TEST));
        }
    }

    @Test
    public void testSimpleContextualAuthWiring() throws Exception {
        // stuff table into the scope
        final String tableName = "testSimpleContextualAuthWiringTest";
        final Table table = TableTools.emptyTable(10).update("I = -i", "J = -i");
        scriptSession.setVariable(tableName, table);

        // export from query scope to our session; this transforms the table
        final Export export = clientSession.session().export(TicketTable.fromQueryScopeField(tableName));

        // verify that we can sort the table prior to the restriction
        clientSession.session().execute(export.table().sort("I"));

        component.authorizationProvider().getTableServiceContextualAuthWiring().delegate =
                new TableServiceContextualAuthWiring.AllowAll() {
                    @Override
                    public void checkPermissionSort(AuthContext authContext, SortTableRequest request,
                            List<Table> sourceTables) {
                        ServiceAuthWiring.operationNotAllowed(DISABLED_FOR_TEST);
                    }
                };

        try {
            clientSession.session().execute(export.table().sort("J"));
            fail("expected the sort to fail");
        } catch (final Exception e) {
            // expect the authorization error details to propagate
            assertTrue(e.getMessage().contains(DISABLED_FOR_TEST));
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

    private static class AnonymousRequestHandler implements AuthenticationRequestHandler {
        @Override
        public String getAuthType() {
            return ANONYMOUS;
        }

        @Override
        public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload,
                AuthenticationRequestHandler.HandshakeResponseListener listener) {
            if (!payload.hasRemaining()) {
                return Optional.of(new AuthContext.Anonymous());
            }
            return Optional.empty();
        }

        @Override
        public Optional<AuthContext> login(String payload,
                AuthenticationRequestHandler.MetadataResponseListener listener) {
            if (payload.isEmpty()) {
                return Optional.of(new AuthContext.Anonymous());
            }
            return Optional.empty();
        }

        @Override
        public void initialize(String targetUrl) {
            // do nothing
        }
    }
}
