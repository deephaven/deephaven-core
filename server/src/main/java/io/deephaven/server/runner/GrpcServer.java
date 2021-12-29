package io.deephaven.server.runner;

import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This interface handles the lifecycle of Netty and Jetty servers in a unified way, while still supporting the use
 * cases that Deephaven expects:
 * <ul>
 * <li>Deephaven wants to initiate stop early in the shutdown process, and block on it after all services have begun to
 * stop.</li>
 * <li>gRPC+Netty supports a non-blocking stop, a "stop now", and a pair of await methods, one of which takes a
 * timeout.</li>
 * <li>gRPC+Jetty supports a blocking stop with a timeout, and a join() method.</li>
 * </ul>
 * In order to not block on the initial stop call, the Jetty implementation will run stop() in another thread. Since
 * Jetty doesn't have an extra "shutdownNow" method, the Netty implementation will use the timeout in another thread to
 * decide if it needs to invoke shutdownNow when normal shutdown is taking too long.
 *
 */
public interface GrpcServer {

    /**
     * Starts the server, if possible. Otherwise, throws an exception. If successful, returns.
     * 
     * @throws IOException if there is an error on startup
     */
    void start() throws IOException;

    /**
     * Blocks as long as the server is running, unless this thread is interrupted. If stopWithTimeout has been called
     * and the timeout has expired, this will return, and the server will be stopped.
     */
    void join() throws InterruptedException;

    /**
     * Stops the server, using the specified timeout as a deadline. Returns immediately. Call {@link #join()} to block
     * until this is completed.
     * 
     * @param timeout time to allow for a graceful shutdown before giving up and halting
     * @param unit unit to apply to the timeout
     */
    void stopWithTimeout(long timeout, TimeUnit unit);

    /**
     * After the server is started, this will return the port it is using.
     * 
     * @return the tcp port that the server is listening on after it has started
     */
    int getPort();

    static GrpcServer of(Server server) {
        return new GrpcServer() {

            @Override
            public void start() throws IOException {
                server.start();
            }

            @Override
            public void join() throws InterruptedException {
                server.awaitTermination();
            }

            @Override
            public void stopWithTimeout(long timeout, TimeUnit unit) {
                server.shutdown();

                // Create and start a thread to make sure we obey the deadline
                Thread shutdownThread = new Thread(() -> {
                    try {
                        if (!server.awaitTermination(timeout, unit)) {
                            server.shutdownNow();
                        }
                    } catch (InterruptedException ignored) {
                    }
                });
                shutdownThread.start();
            }

            @Override
            public int getPort() {
                return server.getPort();
            }
        };
    }
}
