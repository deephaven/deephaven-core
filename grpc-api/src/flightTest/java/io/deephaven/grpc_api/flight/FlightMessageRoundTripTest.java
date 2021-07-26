package io.deephaven.grpc_api.flight;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableDiff;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.grpc_api.arrow.FlightServiceGrpcBinding;
import io.deephaven.grpc_api.auth.AuthContextModule;
import io.deephaven.grpc_api.barrage.BarrageModule;
import io.deephaven.grpc_api.session.SessionModule;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionServiceGrpcImpl;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.grpc.*;
import io.grpc.netty.NettyServerBuilder;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Named;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 * Deliberately much lower in scope (and running time) than BarrageMessageRoundTripTest, the only purpose
 * of this test is to verify that we can round trip
 */
public class FlightMessageRoundTripTest {
    @Singleton
    @Component(modules = {
            BarrageModule.class,
            SessionModule.class,
            AuthContextModule.class
    })
    public interface TestComponent {
        Set<ServerInterceptor> interceptors();

        FlightServiceGrpcBinding flightService();
        SessionServiceGrpcImpl sessionGrpcService();
        SessionService sessionService();

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
    private FlightClient client;
    private UUID sessionToken;
    private SessionState currentSession;

    @Before
    public void setup() throws IOException {
        TestComponent component = DaggerFlightMessageRoundTripTest_TestComponent
                .builder()
                .withScheduler(new Scheduler.DelegatingImpl(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1)))
                .withSessionTokenExpireTmMs(60_000_000)
                .build();

        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(0);
        component.interceptors().forEach(serverBuilder::intercept);
        serverBuilder.addService(component.sessionGrpcService());
        serverBuilder.addService(component.flightService());
        server = serverBuilder.build().start();
        int actualPort = server.getPort();

        client = FlightClient.builder().location(Location.forGrpcInsecure("localhost", actualPort)).allocator(new RootAllocator()).intercept(info -> new FlightClientMiddleware() {
            @Override
            public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
                final UUID currSession = sessionToken;
                if (currSession != null) {
                    outgoingHeaders.insert(SessionServiceGrpcImpl.DEEPHAVEN_SESSION_ID, currSession.toString());
                }
            }
            @Override
            public void onHeadersReceived(CallHeaders incomingHeaders) {
            }
            @Override
            public void onCallCompleted(CallStatus status) {
            }
        }).build();
        SessionServiceGrpc.SessionServiceBlockingStub sessionServiceClient = SessionServiceGrpc.newBlockingStub(ManagedChannelBuilder.forTarget("localhost:" + actualPort)
                .usePlaintext()
                .build());

        HandshakeResponse response = sessionServiceClient.newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build());
        assertNotNull(response.getSessionToken());
        sessionToken = UUID.fromString(response.getSessionToken().toStringUtf8());

        currentSession = component.sessionService().getSessionForToken(sessionToken);
    }
    @After
    public void teardown() {
        server.shutdownNow();
    }

    @Test
    public void testSimpleEmptyTableDoGet() {
        Flight.Ticket simpleTableTicket = ExportTicketHelper.exportIdToTicket(1);
        currentSession.newExport(simpleTableTicket)
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

        // non-flat index
        assertRoundTripDataEqual(TableTools.emptyTable(10).where("i % 2 == 0").update("I=i"));

        // all null values in columns
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=(int)null"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=(String)null"));

        // some nulls in columns
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty= ((i % 2) == 0) ? i : (int)null"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty= ((i % 2) == 0) ? String.valueOf(i) : (String)null"));

        // list columns TODO(#755): support for DBArray
//        assertRoundTripDataEqual(TableTools.emptyTable(5).update("A=i").by().join(TableTools.emptyTable(5)));
    }

    private static int nextTicket = 1;
    private void assertRoundTripDataEqual(Table deephavenTable) throws InterruptedException, ExecutionException {
        // bind the table in the session
        Flight.Ticket dhTableTicket = ExportTicketHelper.exportIdToTicket(nextTicket++);
        currentSession.newExport(dhTableTicket).submit(() -> deephavenTable);

        // fetch with DoGet
        FlightStream stream = client.getStream(new Ticket(dhTableTicket.getTicket().toByteArray()));
        stream.next();
        VectorSchemaRoot root = stream.getRoot();

        // turn data around and send with DoPut
        int flightDescriptorTicketValue = nextTicket++;
        FlightDescriptor descriptor = FlightDescriptor.path("export", flightDescriptorTicketValue + "");
        // start the DoPut and send the schema
        FlightClient.ClientStreamListener putStream = client.startPut(descriptor, root, new AsyncPutListener());
        // send the body of the table
        putStream.putNext();

        // tell the server we are finished sending data
        putStream.completed();
        // block until we're done, so we can get the table and see what is inside
        putStream.getResult();

        // get the table that was uploaded, and confirm it matches what we originally sent
        CompletableFuture<Table> tableFuture = new CompletableFuture<>();
        SessionState.ExportObject<Table> tableExport = currentSession.getExport(flightDescriptorTicketValue);
        currentSession.nonExport()
                .onError(exception -> tableFuture.cancel(true))
                .require(tableExport)
                .submit(() -> tableFuture.complete(tableExport.get()));

        Table uploadedTable = tableFuture.get();

        // check that contents match
        assertEquals(deephavenTable.size(), uploadedTable.size());
        assertEquals(deephavenTable.getDefinition(), uploadedTable.getDefinition());
        assertEquals(0, (long) TableTools.diffPair(deephavenTable, uploadedTable, 0, EnumSet.noneOf(TableDiff.DiffItems.class)).getSecond());
    }
}
