package io.deephaven.server.runner;

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
}
