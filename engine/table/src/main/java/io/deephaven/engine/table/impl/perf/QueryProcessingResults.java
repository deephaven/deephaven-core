/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

public class QueryProcessingResults {

    private final QueryPerformanceRecorder recorder;

    private volatile String exception = null;


    public QueryProcessingResults(final QueryPerformanceRecorder recorder) {
        this.recorder = recorder;
    }

    public String getException() {
        return exception;
    }

    public void setException(String exception) {
        this.exception = exception;
    }

    public QueryPerformanceRecorder getRecorder() {
        return recorder;
    }
}
