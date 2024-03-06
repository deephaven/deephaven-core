//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.tablelogger.Row.Flags;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Objects;

class QueryOperationPerformanceImpl implements QueryOperationPerformanceLogLogger {
    private final QueryOperationPerformanceLogLogger qoplLogger;
    private final QueryOperationPerformanceStreamPublisher publisher;
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public QueryOperationPerformanceImpl(QueryOperationPerformanceLogLogger qoplLogger) {
        this.qoplLogger = Objects.requireNonNull(qoplLogger);
        this.publisher = new QueryOperationPerformanceStreamPublisher();
        this.adapter = new StreamToBlinkTableAdapter(
                QueryOperationPerformanceStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                QueryOperationPerformanceImpl.class.getName());
        this.blink = adapter.table();
    }

    public Table blinkTable() {
        return blink;
    }

    @Override
    public void log(
            @NotNull final Flags flags,
            @NotNull final QueryPerformanceNugget nugget) throws IOException {
        publisher.add(nugget);
        qoplLogger.log(flags, nugget);
    }
}
