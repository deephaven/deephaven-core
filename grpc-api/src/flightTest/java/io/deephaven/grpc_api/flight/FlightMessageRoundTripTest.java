package io.deephaven.grpc_api.flight;

import dagger.BindsInstance;
import dagger.Component;
import io.deephaven.db.tables.Table;
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
import io.deephaven.grpc_api.util.TestControlledScheduler;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.grpc.*;
import io.grpc.CallOptions;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.testing.GrpcServerRule;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Named;
import javax.inject.Singleton;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Deliberately much lower in scope (and running time) than BarrageMessageRoundTripTest, the only purpose
 * of this test is to verify that we can round trip
 */
public class FlightMessageRoundTripTest {

//    @Rule
//    public GrpcServerRule grpcServerRule = new GrpcServerRule();

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
    private FlightClient client;
    private SessionServiceGrpc.SessionServiceBlockingStub sessionServiceClient;
    private UUID sessionToken;
    private SessionState currentSession;

    private class InterceptingManagedChannel extends ManagedChannel {

        private final ClientInterceptor interceptor;
        private final ManagedChannel wrappedManagedChannel;

        private InterceptingManagedChannel(ManagedChannel wrappedManagedChannel, ClientInterceptor interceptor) {
            this.interceptor = interceptor;
            this.wrappedManagedChannel = wrappedManagedChannel;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return interceptor.interceptCall(methodDescriptor, callOptions, wrappedManagedChannel);
        }

        @Override
        public ConnectivityState getState(boolean requestConnection) {
            return wrappedManagedChannel.getState(requestConnection);
        }

        @Override
        public void notifyWhenStateChanged(ConnectivityState source, Runnable callback) {
            wrappedManagedChannel.notifyWhenStateChanged(source, callback);
        }

        @Override
        public void resetConnectBackoff() {
            wrappedManagedChannel.resetConnectBackoff();
        }

        @Override
        public void enterIdle() {
            wrappedManagedChannel.enterIdle();
        }

        @Override
        public ManagedChannel shutdown() {
            return wrappedManagedChannel.shutdown();
        }

        @Override
        public boolean isShutdown() {
            return wrappedManagedChannel.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return wrappedManagedChannel.isTerminated();
        }

        @Override
        public ManagedChannel shutdownNow() {
            return wrappedManagedChannel.shutdownNow();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return wrappedManagedChannel.awaitTermination(timeout, unit);
        }

        @Override
        public String authority() {
            return wrappedManagedChannel.authority();
        }
    }

    private class AuthInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions, final Channel channel) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
                @Override
                public void start(final Listener<RespT> responseListener, final Metadata headers) {
                    final UUID currSession = sessionToken;
                    if (currSession != null) {
                        headers.put(SessionServiceGrpcImpl.SESSION_HEADER_KEY, currSession.toString());
                    }
                    super.start(responseListener, headers);
                }
            };
        }
    }


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
        Server server = serverBuilder.build().start();
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
        sessionServiceClient = SessionServiceGrpc.newBlockingStub(ManagedChannelBuilder.forTarget("localhost:" + actualPort)
                .usePlaintext()
                .build());

        HandshakeResponse response = sessionServiceClient.newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build());
        assertNotNull(response.getSessionToken());
        sessionToken = UUID.fromString(response.getSessionToken().toStringUtf8());

        currentSession = component.sessionService().getSessionForToken(sessionToken);
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

//        root.getSchema().getCustomMetadata();

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
    public void testRoundTripData() throws InterruptedException {
//        assertRoundTripDataEqual(TableTools.emptyTable(0));
//        assertRoundTripDataEqual(TableTools.emptyTable(10));
//        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=(String)null"));
//        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=(int)0"));
        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=\"\""));
//        assertRoundTripDataEqual(TableTools.emptyTable(10).update("empty=\"a\""));
    }
    private static int nextTicket = 1;
    private void assertRoundTripDataEqual(Table deephavenTable) throws InterruptedException {
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
        FlightClient.ClientStreamListener putStream = client.startPut(descriptor, root, new AsyncPutListener());
        putStream.putNext();

        Thread.sleep(1000);
        putStream.completed();
        putStream.getResult();


        Thread.sleep(10_000);
        Table uploadedTable = currentSession.<Table>getExport(flightDescriptorTicketValue).get();

        assertEquals(deephavenTable.size(), uploadedTable.size());
        assertEquals(deephavenTable.getDefinition(), uploadedTable.getDefinition());
    }
}
