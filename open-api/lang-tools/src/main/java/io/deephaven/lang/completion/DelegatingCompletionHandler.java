package io.deephaven.lang.completion;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.lang.parse.ParsedDocument;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.Position;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Allows you to join multiple completion handlers together, and coalesce the final results.
 *
 * In the future, we will better log which handlers provided which completions. We may also simply call the different
 * handlers in a different manner, when the v2 implementation api changes to better support the ChunkerDocument model
 * (and pushing results to client directly), rendering this class obsolete.
 *
 * NOTE: This class is not currently in use, since the coalesced results wound up causing ugly duplications with
 * different whitespace. We will delete it after merging, to have it in git history, in case we decide to revive it
 * later.
 */
public class DelegatingCompletionHandler implements CompletionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingCompletionHandler.class);
    private final CompletionHandler[] handlers;

    public DelegatingCompletionHandler(CompletionHandler... handlers) {
        this.handlers = handlers;
    }

    @Override
    public Collection<CompletionItem.Builder> runCompletion(final ParsedDocument doc, final Position pos,
            final int offset) {
        CompletableFuture<Collection<CompletionItem.Builder>>[] futures = new CompletableFuture[handlers.length];
        for (int i = 0; i < handlers.length; i++) {
            final CompletionHandler handler = handlers[i];
            futures[i] = CompletableFuture.supplyAsync(() -> handler.runCompletion(doc, pos, offset));
        }
        Set<CompletionItem.Builder> all = new LinkedHashSet<>();
        for (CompletableFuture<Collection<CompletionItem.Builder>> future : futures) {
            try {
                all.addAll(future.get(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                LOGGER.trace()
                        .append("Unknown error running autocomplete. ")
                        .append(e.getCause())
                        .endl();
            } catch (TimeoutException e) {
                // yikes, more than 5 seconds? no human alive wants to wait 5 seconds to get autocomplete popup!
                continue;
            }
        }

        return all;
    }

}
