package io.deephaven.lang.completion;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Allows you to join multiple completion handlers together, and coalesce the final results.
 *
 * In the future, we will better log which handlers provided which completions.
 * We may also simply call the different handlers in a different manner,
 * when the v2 implementation api changes to better support the ChunkerDocument model
 * (and pushing results to client directly), rendering this class obsolete.
 *
 * NOTE: This class is not currently in use, since the coalesced results
 * wound up causing ugly duplications with different whitespace.
 * We will delete it after merging, to have it in git history,
 * in case we decide to revive it later.
 */
public class DelegatingCompletionHandler implements CompletionHandler {

    private final CompletionHandler[] handlers;

    public DelegatingCompletionHandler(CompletionHandler ... handlers) {
        this.handlers = handlers;
    }

    @Override
    public CompletableFuture<? extends Collection<CompletionFragment>> complete(String command, int offset) {
        // this is ...kind of silly... we should just be pushing results as they come in.
        // TODO: a new api that supports push, with the future-based api adding blocking around said push messages.  IDS-1517-25
        CompletableFuture[] futures = new CompletableFuture[handlers.length];
        Collection<CompletionFragment>[] results = new Collection[handlers.length];
        for (int i = 0; i < handlers.length; i++) {
            final int slot = i;
            final CompletionHandler handler = handlers[i];
            final CompletableFuture<? extends Collection<CompletionFragment>> started = handler.complete(command, offset);
            futures[i] = CompletableFuture.supplyAsync(()->{
                try {
                    return results[slot] = started.get(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // stay interrupted
                } catch (ExecutionException e) {
                    // log this
                    throw new RuntimeException("Failure in completion handler code for " + handler, e.getCause());
                } catch (TimeoutException e) {
                    // log this
                    throw new RuntimeException("Timeout for completion handler code for " + handler, e.getCause());
                }
                return results[slot] = Collections.emptyList();
            });
        }
        return CompletableFuture.allOf(futures).thenApply(ignored->{
            // coalesce results.  This _really_ should, ideally, just be just pushed to clients...
            Set<CompletionFragment> all = new LinkedHashSet<>();
            for (Collection<CompletionFragment> result : results) {
                all.addAll(result);
            }
            return all;
        });
    }
}
