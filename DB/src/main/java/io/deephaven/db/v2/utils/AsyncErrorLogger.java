/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.tablelogger.RowSetter;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.DynamicTable;

import java.io.IOException;

@SuppressWarnings("unchecked")
public class AsyncErrorLogger {

    private static final DynamicTableWriter tableWriter = new DynamicTableWriter(
        new String[] {"Time", "EvaluationNumber", "OperationNumber", "Description",
                "SourceQueryEvaluationNumber", "SourceQueryOperationNumber",
                "SourceQueryDescription", "Cause", "WorkerName", "HostName"},
        new Class[] {DBDateTime.class, int.class, int.class, String.class, int.class, int.class,
                String.class, Exception.class, String.class, String.class});
    private static final RowSetter<DBDateTime> timeSetter = tableWriter.getSetter("Time");
    private static final RowSetter<Integer> evaluationNumberSetter =
        tableWriter.getSetter("EvaluationNumber");
    private static final RowSetter<Integer> operationNumberSetter =
        tableWriter.getSetter("OperationNumber");
    private static final RowSetter<String> descriptionSetter = tableWriter.getSetter("Description");
    private static final RowSetter<Integer> failingEvaluationNumberSetter =
        tableWriter.getSetter("SourceQueryEvaluationNumber");
    private static final RowSetter<Integer> failingOperationNumberSetter =
        tableWriter.getSetter("SourceQueryOperationNumber");
    private static final RowSetter<String> failingDescriptionSetter =
        tableWriter.getSetter("SourceQueryDescription");
    private static final RowSetter<Throwable> causeSetter = tableWriter.getSetter("Cause");
    private static final RowSetter<String> workerNameSetter = tableWriter.getSetter("WorkerName");
    private static final RowSetter<String> hostNameSetter = tableWriter.getSetter("HostName");

    public static DynamicTable getErrorLog() {
        return tableWriter.getTable();
    }

    public static void log(DBDateTime time, UpdatePerformanceTracker.Entry entry,
        UpdatePerformanceTracker.Entry sourceEntry, Throwable originalException)
        throws IOException {
        timeSetter.set(time);
        if (entry != null) {
            evaluationNumberSetter.set(entry.getEvaluationNumber());
            operationNumberSetter.setInt(entry.getOperationNumber());
            descriptionSetter.set(entry.getDescription());
        }
        if (sourceEntry != null) {
            failingEvaluationNumberSetter.set(sourceEntry.getEvaluationNumber());
            failingOperationNumberSetter.setInt(sourceEntry.getOperationNumber());
            failingDescriptionSetter.set(sourceEntry.getDescription());
        }
        // TODO (deephaven/deephaven-core/issues/159): Do we continue supporting this? If so, we
        // should consider fixing host name and worker name.
        workerNameSetter.set(null);
        hostNameSetter.set(null);
        causeSetter.set(originalException);
        tableWriter.writeRow();
    }
}

