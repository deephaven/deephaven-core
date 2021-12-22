package io.deephaven.server.netty;

import io.deephaven.server.test.FlightMessageRoundTripTest;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;

import java.io.IOException;

public class FlightRoundTripTest extends FlightMessageRoundTripTest {
    @Override
    protected int startServer(TestComponent component) throws IOException  {
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(0);
        component.interceptors().forEach(serverBuilder::intercept);
        serverBuilder.addService(component.sessionGrpcService());
        serverBuilder.addService(component.flightService());
        Server server = serverBuilder.build().start();
        return server.getPort();
    }
}
