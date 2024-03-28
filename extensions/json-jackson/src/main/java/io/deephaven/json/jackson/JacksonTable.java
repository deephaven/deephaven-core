//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.JsonTableOptions;
import io.deephaven.json.Source;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.util.thread.NamingThreadFactory;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 */
public final class JacksonTable {

    /**
     * Creates a blink table according to the {@code options} and {@code factory} by combining a
     * {@link JacksonStreamPublisher} and {@link StreamToBlinkTableAdapter}.
     *
     * @param options the options
     * @param factory the jackson factory
     * @return the blink table
     */
    public static Table of(JsonTableOptions options, JsonFactory factory) {
        final int numThreads = Math.min(options.maxThreads(), options.sources().size());
        final ExecutorService executor = executor(options, numThreads);
        try {
            return of(options, factory, executor, numThreads);
        } finally {
            // ensure no new tasks can be added, but does not cancel existing submissions
            executor.shutdown();
        }
    }

    private static ExecutorService executor(JsonTableOptions options, int numThreads) {
        final NamingThreadFactory threadFactory =
                new NamingThreadFactory(null, JacksonTable.class, options.name(), true);
        return Executors.newFixedThreadPool(numThreads, threadFactory);
    }

    private static Table of(JsonTableOptions options, JsonFactory factory, Executor executor, int numJobs) {
        final JacksonStreamPublisher publisher = JacksonStreamPublisher.of(JsonStreamPublisherOptions.builder()
                .options(options.options())
                .multiValueSupport(options.multiValueSupport())
                .chunkSize(options.chunkSize())
                .build(),
                factory);
        final TableDefinition tableDefinition = publisher.tableDefinition(options.namingFunction());
        // noinspection resource
        final StreamToBlinkTableAdapter adapter = new StreamToBlinkTableAdapter(tableDefinition, publisher,
                options.updateSourceRegistrar(), options.name(), options.extraAttributes(), true);
        // Even if there's a single thread, probably best to use array blocking queue (instead of LinkedList
        // alternative)
        final Queue<Source> queue = new ArrayBlockingQueue<>(options.sources().size(), false, options.sources());
        for (int i = 0; i < numJobs; ++i) {
            publisher.execute(executor, queue);
        }
        return adapter.table();
    }
}
