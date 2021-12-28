package io.deephaven.server.runner;

import dagger.Module;
import dagger.Provides;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.grpc.BindableService;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Module
public class ServerBuilderInProcessModule {

    private static final Set<String> DRAIN_METHODS =
            Stream.of("arrow.flight.protocol.FlightService/DoGet", "arrow.flight.protocol.FlightService/DoExchange")
                    .collect(Collectors.toSet());

    @Singleton
    @Provides
    @Named("serverName")
    static String serverName() {
        return InProcessServerBuilder.generateName();
    }

    @Provides
    static GrpcServer serverBuilder(@Named("serverName") String serverName, Set<BindableService> services,
            Set<ServerInterceptor> interceptors) {
        InProcessServerBuilder builder =
                InProcessServerBuilder.forName(serverName).intercept(DrainingInterceptor.INSTANCE);

        services.forEach(builder::addService);
        interceptors.forEach(builder::intercept);

        return GrpcServer.of(builder.build());
    }

    @Provides
    static ManagedChannelBuilder<?> channelBuilder(@Named("serverName") String serverName) {
        return InProcessChannelBuilder.forName(serverName);
    }

    /**
     * Deephaven has some defensive implementations of Drainable / InputStream that don't actually implement the read
     * methods of InputStream. This works when going over the wire, but when doing an in-process exchange, the defensive
     * bits get tripped. To avoid this, we need to capture the bytes via Drainable.
     *
     * @see DefensiveDrainable
     */
    private enum DrainingInterceptor implements ServerInterceptor {
        INSTANCE;

        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            if (DRAIN_METHODS.contains(call.getMethodDescriptor().getFullMethodName())) {
                return next.startCall(new Draining<>(call), headers);
            }
            return next.startCall(call, headers);
        }
    }

    private static class Draining<ReqT, RespT> extends SimpleForwardingServerCall<ReqT, RespT> {
        private Draining(ServerCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public void sendMessage(RespT in) {
            if (in instanceof DefensiveDrainable) {
                final InputStream capture = ((DefensiveDrainable) in).capture();
                // noinspection unchecked
                super.sendMessage((RespT) capture);
            } else {
                super.sendMessage(in);
            }
        }
    }
}
