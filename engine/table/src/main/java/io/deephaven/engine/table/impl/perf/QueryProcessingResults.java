/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.perf;

import io.deephaven.util.QueryConstants;

import java.io.Serializable;

public class QueryProcessingResults implements Serializable {

    private static final long serialVersionUID = 2L;

    private final QueryPerformanceRecorder recorder;

    private volatile Boolean isReplayer = QueryConstants.NULL_BOOLEAN;
    private volatile String exception = null;


    public QueryProcessingResults(final QueryPerformanceRecorder recorder) {
        this.recorder = recorder;
    }

    public Boolean isReplayer() {
        return isReplayer;
    }

    public void setReplayer(Boolean replayer) {
        isReplayer = replayer;
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
