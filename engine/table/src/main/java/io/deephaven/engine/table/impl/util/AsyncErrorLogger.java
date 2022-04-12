/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.time.DateTime;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.tablelogger.RowSetter;

import java.io.IOException;

@SuppressWarnings("unchecked")
public class AsyncErrorLogger {

    private static final DynamicTableWriter tableWriter = new DynamicTableWriter(
            TableHeader.of(
                    ColumnHeader.of("Time", DateTime.class),
                    ColumnHeader.ofInt("EvaluationNumber"),
                    ColumnHeader.ofInt("OperationNumber"),
                    ColumnHeader.ofString("Description"),
                    ColumnHeader.ofInt("SourceQueryEvaluationNumber"),
                    ColumnHeader.ofInt("SourceQueryOperationNumber"),
                    ColumnHeader.ofString("SourceQueryDescription"),
                    ColumnHeader.of("Cause", Exception.class),
                    ColumnHeader.ofString("WorkerName"),
                    ColumnHeader.ofString("HostName")));
    private static final RowSetter<DateTime> timeSetter = tableWriter.getSetter("Time");
    private static final RowSetter<Integer> evaluationNumberSetter = tableWriter.getSetter("EvaluationNumber");
    private static final RowSetter<Integer> operationNumberSetter = tableWriter.getSetter("OperationNumber");
    private static final RowSetter<String> descriptionSetter = tableWriter.getSetter("Description");
    private static final RowSetter<Integer> failingEvaluationNumberSetter =
            tableWriter.getSetter("SourceQueryEvaluationNumber");
    private static final RowSetter<Integer> failingOperationNumberSetter =
            tableWriter.getSetter("SourceQueryOperationNumber");
    private static final RowSetter<String> failingDescriptionSetter = tableWriter.getSetter("SourceQueryDescription");
    private static final RowSetter<Throwable> causeSetter = tableWriter.getSetter("Cause");
    private static final RowSetter<String> workerNameSetter = tableWriter.getSetter("WorkerName");
    private static final RowSetter<String> hostNameSetter = tableWriter.getSetter("HostName");

    public static Table getErrorLog() {
        return tableWriter.getTable();
    }

    public static void log(DateTime time, TableListener.Entry entry,
            TableListener.Entry sourceEntry, Throwable originalException) throws IOException {
        timeSetter.set(time);
        if (entry instanceof PerformanceEntry) {
            final PerformanceEntry uptEntry = (PerformanceEntry) entry;
            evaluationNumberSetter.set(uptEntry.getEvaluationNumber());
            operationNumberSetter.setInt(uptEntry.getOperationNumber());
            descriptionSetter.set(uptEntry.getDescription());
        }
        if (sourceEntry instanceof PerformanceEntry) {
            final PerformanceEntry uptSourceEntry = (PerformanceEntry) sourceEntry;
            failingEvaluationNumberSetter.set(uptSourceEntry.getEvaluationNumber());
            failingOperationNumberSetter.setInt(uptSourceEntry.getOperationNumber());
            failingDescriptionSetter.set(uptSourceEntry.getDescription());
        }
        // TODO (deephaven/deephaven-core/issues/159): Do we continue supporting this? If so, we should consider fixing
        // host name and worker name.
        workerNameSetter.set(null);
        hostNameSetter.set(null);
        causeSetter.set(originalException);
        tableWriter.writeRow();
    }
}

