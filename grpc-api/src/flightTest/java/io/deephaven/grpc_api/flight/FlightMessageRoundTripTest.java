package io.deephaven.grpc_api.flight;

import dagger.BindsInstance;
import dagger.Component;
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
import io.grpc.testing.GrpcServerRule;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.inject.Named;
import javax.inject.Singleton;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

/**
 * Deliberately much lower in scope (and running time) than BarrageMessageRoundTripTest, the only purpose
 * of this test is to verify that we can round trip
 */
public class FlightMessageRoundTripTest {

    @Rule
    public GrpcServerRule grpcServerRule = new GrpcServerRule();

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
    public void setup() {
        TestComponent component = DaggerFlightMessageRoundTripTest_TestComponent
                .builder()
                .withScheduler(new Scheduler.DelegatingImpl(Executors.newSingleThreadExecutor(), Executors.newScheduledThreadPool(1)))
                .withSessionTokenExpireTmMs(60_000)
                .build();

        ManagedChannel clientChannel = new InterceptingManagedChannel(grpcServerRule.getChannel(), new AuthInterceptor());

        client = FlightGrpcUtils.createFlightClient(new RootAllocator(), clientChannel);
        sessionServiceClient = SessionServiceGrpc.newBlockingStub(clientChannel);

        Set<ServerInterceptor> interceptors = component.interceptors();
        grpcServerRule.getServiceRegistry().addService(ServerInterceptors.intercept(component.flightService(), interceptors.toArray(new ServerInterceptor[0])));
        grpcServerRule.getServiceRegistry().addService(component.sessionGrpcService());

        HandshakeResponse response = sessionServiceClient.newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build());
        assertNotNull(response.getSessionToken());
        sessionToken = UUID.fromString(response.getSessionToken().toStringUtf8());

        currentSession = component.sessionService().getSessionForToken(sessionToken);
    }
    @Test
    public void testNothing() {
        Flight.Ticket simpleTableTicket = ExportTicketHelper.exportIdToTicket(1);
        currentSession.newExport(simpleTableTicket)
                .submit(() -> TableTools.emptyTable(10).update("I=i"));

        client.getStream(new Ticket(simpleTableTicket.getTicket().toByteArray())).next();
    }
}
