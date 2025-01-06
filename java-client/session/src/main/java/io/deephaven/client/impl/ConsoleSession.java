//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.client.impl.script.Changes;
import io.deephaven.proto.backplane.grpc.Ticket;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A console session is part of a {@link Session} that allows script execution.
 */
public interface ConsoleSession extends Closeable {
    /**
     * The console script type.
     *
     * @return the type
     */
    String type();

    /**
     * The export ticket for {@code this} console session.
     *
     * @return the ticket
     */
    Ticket ticket();

    /**
     * Execute the given {@code code} against the script session.
     *
     * @param code the code
     * @return the changes
     * @throws InterruptedException if the current thread is interrupted
     * @throws ExecutionException if the request has an exception
     * @throws TimeoutException if the request times out
     */
    default Changes executeCode(String code) throws InterruptedException, ExecutionException, TimeoutException {
        return executeCode(code, false);
    }

    /**
     * Execute the given {@code code} against the script session. The code may be executed systemically, meaning that
     * failures of the executed script constitute failures of the application and cause shut down.
     *
     * @param code the code
     * @param systemic if the code should be executed systemically.
     * @return the changes
     * @throws InterruptedException if the current thread is interrupted
     * @throws ExecutionException if the request has an exception
     * @throws TimeoutException if the request times out
     */
    Changes executeCode(String code, boolean systemic)
            throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Execute the given {@code path path's} code against the script session.
     *
     * @param path the path to the code
     * @return the changes
     * @throws InterruptedException if the current thread is interrupted
     * @throws ExecutionException if the request has an exception
     * @throws TimeoutException if the request times out
     */
    default Changes executeScript(Path path)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        return executeScript(path, false);
    }

    /**
     * Execute the given {@code path path's} code against the script session.
     *
     * @param path the path to the code
     * @return the changes
     * @throws InterruptedException if the current thread is interrupted
     * @throws ExecutionException if the request has an exception
     * @throws TimeoutException if the request times out
     */
    Changes executeScript(Path path, boolean systemic)
            throws IOException, InterruptedException, ExecutionException, TimeoutException;

    /**
     * Execute the given {@code code} against the script session.
     *
     * @param code the code
     * @return the changes future
     */
    default CompletableFuture<Changes> executeCodeFuture(String code) {
        return executeCodeFuture(code, false);
    }

    /**
     * Execute the given {@code code} against the script session.
     *
     * @param code the code
     * @return the changes future
     */
    CompletableFuture<Changes> executeCodeFuture(String code, boolean systemic);

    /**
     * Execute the given {@code path path's} code against the script session.
     *
     * @param path the path to the code
     * @return the changes future
     */
    default CompletableFuture<Changes> executeScriptFuture(Path path) throws IOException {
        return executeScriptFuture(path, false);
    }

    /**
     * Execute the given {@code path path's} code against the script session.
     *
     * @param path the path to the code
     * @return the changes future
     */
    CompletableFuture<Changes> executeScriptFuture(Path path, boolean systemic) throws IOException;

    /**
     * Closes {@code this} console session.
     */
    @Override
    void close();

    /**
     * Closes {@code this} console session.
     *
     * @return the future
     */
    CompletableFuture<Void> closeFuture();
}
