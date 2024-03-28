//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.stream.StreamPublisher;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.function.Function;

public interface JsonStreamPublisher extends StreamPublisher {

    // Stream<? extends Type<?>> types();

    // todo:
    TableDefinition tableDefinition(Function<List<String>, String> namingFunction);

    /**
     * Adds a new job into {@code executor} that {@link Queue#poll() polls} from {@code sources} and process them as
     * appropriate. The job will continue until {@code sources} becomes empty. It is not typically appropriate for
     * callers to enqueue into {@code sources} after the return of this method, as there is no guarantee that the job
     * will not have already finished. Callers may invoke this method with the same {@code executor} and {@code sources}
     * multiple times, assuming that {@code sources} is thread-safe.
     *
     * @param executor the executor
     * @param sources the sources
     */
    void execute(Executor executor, Queue<Source> sources);
}
