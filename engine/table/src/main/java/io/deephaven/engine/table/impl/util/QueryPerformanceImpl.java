/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryProcessingResults;
import io.deephaven.engine.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.process.ProcessUniqueId;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.tablelogger.Row.Flags;

import java.io.IOException;
import java.util.Objects;

class QueryPerformanceImpl implements QueryPerformanceLogLogger {
    private final ProcessUniqueId id;
    private final QueryPerformanceLogLogger qplLogger;
    private final QueryPerformanceStreamPublisher publisher;
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public QueryPerformanceImpl(ProcessUniqueId id, QueryPerformanceLogLogger qplLogger) {
        this.id = Objects.requireNonNull(id);
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
    public void log(Flags flags, QueryProcessingResults queryProcessingResults,
            QueryPerformanceNugget nugget) throws IOException {
        publisher.add(id.value(), queryProcessingResults, nugget);
        qplLogger.log(flags, queryProcessingResults, nugget);
    }
}
