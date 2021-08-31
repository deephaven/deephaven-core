package io.deephaven.client.impl;

import io.deephaven.qst.table.TableSpec;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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
     * Creates a new console session of script type {@code type}.
     *
     * <p>
     * Note: the server does not currently support independent console sessions. See
     * <a href="https://github.com/deephaven/deephaven-core/issues/1172">Issue 1172</a>.
     *
     * @param type the script type
     * @return the console session future
     */
    CompletableFuture<? extends ConsoleSession> console(String type);

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
