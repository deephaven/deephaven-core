package io.deephaven.server.runner;

import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This is a partial implementation of io.grpc.Server, with enough detail for a Deephaven server to turn it off, whether
 * it is backed by netty or some servlet container, etc.
 */
public interface GrpcServer {

    void start() throws IOException;

    void shutdown();

    void shutdownNow();

    void awaitTermination() throws InterruptedException;

    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    static GrpcServer of(Server server) {
        return new GrpcServer() {

            @Override
            public void start() throws IOException {
                server.start();
            }

            @Override
            public void shutdown() {
                server.shutdown();
            }

            @Override
            public void shutdownNow() {
                server.shutdownNow();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return server.awaitTermination(timeout, unit);
            }

            @Override
            public void awaitTermination() throws InterruptedException {
                server.awaitTermination();
            }
        };
    }
}
