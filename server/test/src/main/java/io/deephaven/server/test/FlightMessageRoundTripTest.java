package io.deephaven.server.test;

import dagger.BindsInstance;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import dagger.multibindings.IntoSet;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.grpc_api.util.FlightExportTicketHelper;
import io.deephaven.grpc_api.util.ScopeTicketHelper;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.arrow.FlightServiceGrpcBinding;
import io.deephaven.server.auth.AuthContextModule;
import io.deephaven.server.console.GlobalSessionProvider;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionServiceGrpcImpl;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketResolver;
import io.deephaven.server.util.Scheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyServerBuilder;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Deliberately much lower in scope (and running time) than BarrageMessageRoundTripTest, the only purpose of this test
 * is to verify that we can round trip
 */
public abstract class FlightMessageRoundTripTest {
    @Module
    public static class FlightTestModule {
        @IntoSet
        @Provides
        TicketResolver ticketResolver(ScopeTicketResolver resolver) {
            return resolver;
        }

        @Provides
        AbstractScriptSession createGlobalScriptSession(GlobalSessionProvider sessionProvider) {
            final AbstractScriptSession scriptSession = new NoLanguageDeephavenSession("non-script-session");
            sessionProvider.initializeGlobalScriptSession(scriptSession);
            return scriptSession;
        }
    }

    @Singleton
    @Component(modules = {
            FlightTestModule.class,
            ArrowModule.class,
            SessionModule.class,
            AuthContextModule.class
    })
    public interface TestComponent {
        Set<ServerInterceptor> interceptors();

        FlightServiceGrpcBinding flightService();

        SessionServiceGrpcImpl sessionGrpcService();

        SessionService sessionService();

        AbstractScriptSession scriptSession();

        @Component.Builder
        interface Builder {
            @BindsInstance
            Builder withScheduler(final Scheduler scheduler);

            @BindsInstance
            Builder withSessionTokenExpireTmMs(@Named("session.tokenExpireMs") long tokenExpireMs);

            TestComponent build();
        }
    }

    private Server server;

    private ManagedChannel channel;
    private FlightClient client;

    private UUID sessionToken;
    private SessionState currentSession;
    private AbstractScriptSession scriptSession;

    @Before
    public void setup() throws IOException {
        TestComponent component = DaggerFlightMessageRoundTripTest_TestComponent
                .builder()
                .withScheduler(new Scheduler.DelegatingImpl(Executors.newSingleThreadExecutor(),
                        Executors.newScheduledThreadPool(1)))
                .withSessionTokenExpireTmMs(60_000_000)
                .build();

        int actualPort = startServer(component);

        scriptSession = component.scriptSession();

        client = FlightClient.builder().location(Location.forGrpcInsecure("localhost", actualPort))
                .allocator(new RootAllocator()).intercept(info -> new FlightClientMiddleware() {
                    @Override
                    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                        final UUID currSession = sessionToken;
                        if (currSession != null) {
                            outgoingHeaders.insert(SessionServiceGrpcImpl.DEEPHAVEN_SESSION_ID, currSession.toString());
                        }
                    }

                    @Override
                    public void onHeadersReceived(CallHeaders incomingHeaders) {}

                    @Override
                    public void onCallCompleted(CallStatus status) {}
                }).build();
        channel = ManagedChannelBuilder.forTarget("localhost:" + actualPort)
                .usePlaintext()
                .build();
        SessionServiceGrpc.SessionServiceBlockingStub sessionServiceClient =
                SessionServiceGrpc.newBlockingStub(channel);

        HandshakeResponse response =
                sessionServiceClient.newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build());
        assertNotNull(response.getSessionToken());
        sessionToken = UUID.fromString(response.getSessionToken().toStringUtf8());

        currentSession = component.sessionService().getSessionForToken(sessionToken);
    }

    protected /* abstract */ int startServer(
            io.deephaven.server.test.FlightMessageRoundTripTest.TestComponent component) throws IOException {
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(0);
        component.interceptors().forEach(serverBuilder::intercept);
        serverBuilder.addService(component.sessionGrpcService());
        serverBuilder.addService(component.flightService());
        server = serverBuilder.build().start();
        return server.getPort();
    }

    @After
    public void teardown() {
        scriptSession.release();

        channel.shutdown();
        server.shutdown();

        try {
            channel.awaitTermination(1, TimeUnit.MINUTES);
            server.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            channel.shutdownNow();
            channel = null;

            server.shutdownNow();
            server = null;
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

    @Test
    public void testSimpleEmptyTableDoGet() {
        Flight.Ticket simpleTableTicket = FlightExportTicketHelper.exportIdToFlightTicket(1);
        currentSession.newExport(simpleTableTicket, "")
                .submit(() -> TableTools.emptyTable(10).update("I=i"));

        FlightStream stream = client.getStream(new Ticket(simpleTableTicket.getTicket().toByteArray()));
        assertTrue(stream.next());
        VectorSchemaRoot root = stream.getRoot();
        // row count should match what we expect
        assertEquals(10, root.getRowCount());

        // only one column was sent
        assertEquals(1, root.getFieldVectors().size());
        Field i = root.getSchema().findField("I");

        // all DH columns are nullable, even primitives
        assertTrue(i.getFieldType().isNullable());
        // verify it is a java int type, which is an arrow 32bit int
        assertEquals(ArrowType.ArrowTypeID.Int, i.getFieldType().getType().getTypeID());
        assertEquals(32, ((ArrowType.Int) i.getFieldType().getType()).getBitWidth());
        assertEquals("int", i.getMetadata().get("deephaven:type"));

        // verify that the server didn't send more data after the first payload
        assertFalse(stream.next());
    }

    @Test
    public void testRoundTripData() throws InterruptedException, ExecutionException {
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

        // list columns TODO(#755): support for Vector
        // assertRoundTripDataEqual(TableTools.emptyTable(5).update("A=i").groupBy().join(TableTools.emptyTable(5)));
    }

    @Test
    public void testTimestampColumn() throws InterruptedException, ExecutionException {
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("tm = DateTime.now()"));
    }

    @Test
    public void testStringCol() throws InterruptedException, ExecutionException {
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
    public void testLongCol() throws InterruptedException, ExecutionException {
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
        assertInfoMatchesTable(client.getInfo(arrowFlightDescriptorForName(staticTableName)), table);
        assertInfoMatchesTable(client.getInfo(arrowFlightDescriptorForName(tickingTableName)), tickingTable);

        // test list flights which runs through scoped tickets
        final MutableInt seenTables = new MutableInt();
        client.listFlights(Criteria.ALL).forEach(fi -> {
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
            assertSchemaMatchesTable(client.getSchema(arrowFlightDescriptorForName(staticTableName)).getSchema(),
                    table);
            assertSchemaMatchesTable(client.getSchema(arrowFlightDescriptorForName(tickingTableName)).getSchema(),
                    tickingTable);

            // test list flights which runs through scoped tickets
            final MutableInt seenTables = new MutableInt();
            client.listFlights(Criteria.ALL).forEach(fi -> {
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
        currentSession.newExport(ticket, "").submit(() -> table);

        // test fetch info from export ticket
        final FlightInfo info = client.getInfo(FlightDescriptor.path("export", "1"));
        assertInfoMatchesTable(info, table);

        // test list flights which runs through scoped tickets
        client.listFlights(Criteria.ALL).forEach(fi -> {
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
        Assert.eq(schema.getFields().size(), "schema.getFields().size()", table.getColumns().length,
                "table.getColumns().length");
        Assert.equals(BarrageUtil.convertArrowSchema(schema).tableDef,
                "BarrageUtil.convertArrowSchema(schema)",
                table.getDefinition(), "table.getDefinition()");
    }

    private static int nextTicket = 1;

    private void assertRoundTripDataEqual(Table deephavenTable) throws InterruptedException, ExecutionException {
        // bind the table in the session
        Flight.Ticket dhTableTicket = FlightExportTicketHelper.exportIdToFlightTicket(nextTicket++);
        currentSession.newExport(dhTableTicket, "").submit(() -> deephavenTable);

        // fetch with DoGet
        FlightStream stream = client.getStream(new Ticket(dhTableTicket.getTicket().toByteArray()));
        VectorSchemaRoot root = stream.getRoot();

        // start the DoPut and send the schema
        int flightDescriptorTicketValue = nextTicket++;
        FlightDescriptor descriptor = FlightDescriptor.path("export", flightDescriptorTicketValue + "");
        FlightClient.ClientStreamListener putStream = client.startPut(descriptor, root, new AsyncPutListener());

        // send the body of the table
        while (stream.next()) {
            putStream.putNext();
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
}
