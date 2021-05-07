package io.deephaven.lang.completion;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * General API for returning a list of completion results from a given offset in a source command.
 */
public interface CompletionHandler {
    CompletableFuture<? extends Collection<CompletionFragment>> complete(String command, int offset);
}
