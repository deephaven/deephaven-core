/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.tablelogger.Row.Flags;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Objects;

class QueryPerformanceImpl implements QueryPerformanceLogLogger {
    private final QueryPerformanceLogLogger qplLogger;
    private final QueryPerformanceStreamPublisher publisher;
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public QueryPerformanceImpl(QueryPerformanceLogLogger qplLogger) {
        this.qplLogger = Objects.requireNonNull(qplLogger);
        this.publisher = new QueryPerformanceStreamPublisher();
        this.adapter = new StreamToBlinkTableAdapter(
                QueryPerformanceStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                QueryPerformanceImpl.class.getName());
        this.blink = adapter.table();
    }

    public Table blinkTable() {
        return blink;
    }

    @Override
    public void log(
            @NotNull final Flags flags,
            @NotNull final QueryPerformanceNugget nugget,
            @Nullable final Exception exception) throws IOException {
        publisher.add(nugget, exception);
        qplLogger.log(flags, nugget, exception);
    }
}
