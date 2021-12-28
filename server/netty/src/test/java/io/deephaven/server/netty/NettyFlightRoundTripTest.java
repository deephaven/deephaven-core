package io.deephaven.server.netty;

import dagger.Component;
import io.deephaven.server.arrow.ArrowModule;
import io.deephaven.server.auth.AuthContextModule;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.server.session.SessionModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import javax.inject.Singleton;
import java.io.IOException;

public class NettyFlightRoundTripTest extends FlightMessageRoundTripTest {
    @Singleton
    @Component(modules = {
            FlightTestModule.class,
            ArrowModule.class,
            SessionModule.class,
            AuthContextModule.class,
            NettyServerModule.class
    })
    public interface NettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerNettyFlightRoundTripTest_NettyTestComponent.create();
    }
}
