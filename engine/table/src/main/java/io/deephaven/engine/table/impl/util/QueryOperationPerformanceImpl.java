/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.process.ProcessUniqueId;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.tablelogger.Row.Flags;

import java.io.IOException;
import java.util.Objects;

class QueryOperationPerformanceImpl implements QueryOperationPerformanceLogLogger {
    private final ProcessUniqueId id;
    private final QueryOperationPerformanceLogLogger qoplLogger;
    private final QueryOperationPerformanceStreamPublisher publisher;
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public QueryOperationPerformanceImpl(ProcessUniqueId id, QueryOperationPerformanceLogLogger qoplLogger) {
        this.id = Objects.requireNonNull(id);
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
    public void log(Flags flags, int operationNumber, QueryPerformanceNugget nugget) throws IOException {
        publisher.add(id.value(), operationNumber, nugget);
        qoplLogger.log(flags, operationNumber, nugget);
    }
}
