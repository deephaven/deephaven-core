/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;

import javax.annotation.Nullable;
import java.time.Instant;

class AsyncErrorImpl {
    private final AsyncErrorStreamPublisher publisher;
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public AsyncErrorImpl() {
        publisher = new AsyncErrorStreamPublisher();
        adapter = new StreamToBlinkTableAdapter(
                AsyncErrorStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                AsyncErrorImpl.class.getName());
        blink = adapter.table();
    }

    public Table blink() {
        return blink;
    }

    public void add(
            Instant time,
            @Nullable TableListener.Entry entry,
            @Nullable TableListener.Entry sourceEntry,
            Throwable originalException) {
        final int evaluationNumber;
        final int operationNumber;
        final String description;
        if (entry instanceof PerformanceEntry) {
            final PerformanceEntry uptEntry = (PerformanceEntry) entry;
            evaluationNumber = uptEntry.getEvaluationNumber();
            operationNumber = uptEntry.getOperationNumber();
            description = uptEntry.getDescription();
        } else {
            evaluationNumber = QueryConstants.NULL_INT;
            operationNumber = QueryConstants.NULL_INT;
            description = null;
        }
        final int sourceEvaluationNumber;
        final int sourceOperationNumber;
        final String sourceDescription;
        if (sourceEntry instanceof PerformanceEntry) {
            final PerformanceEntry uptEntry = (PerformanceEntry) sourceEntry;
            sourceEvaluationNumber = uptEntry.getEvaluationNumber();
            sourceOperationNumber = uptEntry.getOperationNumber();
            sourceDescription = uptEntry.getDescription();
        } else {
            sourceEvaluationNumber = QueryConstants.NULL_INT;
            sourceOperationNumber = QueryConstants.NULL_INT;
            sourceDescription = null;
        }
        publisher.add(
                DateTimeUtils.epochNanos(time),
                evaluationNumber,
                operationNumber,
                description,
                sourceEvaluationNumber,
                sourceOperationNumber,
                sourceDescription,
                originalException);
    }
}
