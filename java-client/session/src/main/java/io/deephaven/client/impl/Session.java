package io.deephaven.client.impl;

import io.deephaven.client.impl.script.Changes;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandResponse;
import io.deephaven.qst.table.TableSpec;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * A session represents a client-side connection to a Deephaven server.
 */
public interface Session extends AutoCloseable, TableHandleManager {

    /**
     * Creates a new export with a {@link io.deephaven.client.impl.ExportRequest.Listener#logging() logging listener}.
     *
     * <p>
     * Equivalent to {@code export(ExportsRequest.logging(table)).get(0)}.
     *
     * @param table the table
     * @return the export
     */
    Export export(TableSpec table);

    /**
     * Creates new exports according to the {@code request}.
     * 
     * @param request the request
     * @return the exports
     */
    List<Export> export(ExportsRequest request);

    // ----------------------------------------------------------

    /**
     * Execute the given {@code code} against the script session.
     *
     * @param code the code
     * @return the changes
     * @throws InterruptedException if the current thread is interrupted
     * @throws ExecutionException if the request has an exception
     * @throws TimeoutException if the request times out
     */
    Changes executeCode(String code) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Execute the given {@code path path's} code against the script session.
     *
     * @param path the path to the code
     * @return the changes
     * @throws InterruptedException if the current thread is interrupted
     * @throws ExecutionException if the request has an exception
     * @throws TimeoutException if the request times out
     */
    Changes executeScript(Path path) throws IOException, InterruptedException, ExecutionException, TimeoutException;

    /**
     * Execute the given {@code code} against the script session.
     *
     * @param code the code
     * @return the changes future
     */
    CompletableFuture<Changes> executeCodeFuture(String code);

    /**
     * Execute the given {@code path path's} code against the script session.
     *
     * @param path the path to the code
     * @return the changes future
     */
    CompletableFuture<Changes> executeScriptFuture(Path path) throws IOException;

    // ----------------------------------------------------------

    /**
     * Publishes {@code export} into the global scope with {@code name}.
     *
     * @param name the name, must conform to {@link javax.lang.model.SourceVersion#isName(CharSequence)}
     * @param export the export
     * @return the publish completable future
     */
    CompletableFuture<Void> publish(String name, Export export);

    /**
     * Closes the session.
     */
    @Override
    void close();

    /**
     * Closes the session.
     *
     * @return the future
     */
    CompletableFuture<Void> closeFuture();

    /**
     * A batch table handle manager.
     *
     * @return a batch manager
     */
    TableHandleManager batch();

    /**
     * A batch table handle manager.
     *
     * @param mixinStacktraces if stacktraces should be mixin
     * @return a batch manager
     */
    TableHandleManager batch(boolean mixinStacktraces);

    /**
     * A serial table handle manager.
     *
     * @return a serial manager
     */
    TableHandleManager serial();
}
