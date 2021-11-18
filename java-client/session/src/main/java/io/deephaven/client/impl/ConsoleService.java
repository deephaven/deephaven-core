package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface ConsoleService {
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

    /**
     * Publishes {@code ticket} into the global scope with {@code name}.
     *
     * @param name the name, must conform to {@link javax.lang.model.SourceVersion#isName(CharSequence)}
     * @param ticketId the export ID
     * @return the publish completable future
     */
    CompletableFuture<Void> publish(String name, HasTicketId ticketId);
}
