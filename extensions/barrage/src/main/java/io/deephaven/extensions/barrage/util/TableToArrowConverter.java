//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.deephaven.extensions.barrage.BarrageStreamGeneratorImpl;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

import static io.deephaven.extensions.barrage.util.BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS;
import static io.deephaven.extensions.barrage.util.BarrageUtil.schemaBytesFromTable;

/**
 * This class produces Arrow Ipc Messages for a Deephaven table, including the schema and all columnar data. The data is
 * split into chunks and returned as multiple Arrow RecordBatch messages.
 */
public class TableToArrowConverter {
    private final BaseTable table;
    private ArrowBuilderObserver listener = null;

    public TableToArrowConverter(BaseTable table) {
        this.table = table;
    }

    private void populateRecordBatches() {
        if (listener != null) {
            return;
        }

        final BarragePerformanceLog.SnapshotMetricsHelper metrics =
                new BarragePerformanceLog.SnapshotMetricsHelper();
        listener = new ArrowBuilderObserver();
        BarrageUtil.createAndSendSnapshot(new BarrageStreamGeneratorImpl.ArrowFactory(), table, null, null,
                false, DEFAULT_SNAPSHOT_DESER_OPTIONS, listener, metrics);
    }

    public byte[] getSchema() {
        return schemaBytesFromTable(table).toByteArray();
    }

    public boolean hasNext() {
        populateRecordBatches();
        return !listener.batchMessages.isEmpty();
    }

    public byte[] next() {
        populateRecordBatches();
        if (listener.batchMessages.isEmpty()) {
            throw new NoSuchElementException("There are no more RecordBatches for the table");
        }
        return listener.batchMessages.pop();
    }

    private static class ArrowBuilderObserver implements StreamObserver<BarrageStreamGenerator.MessageView> {
        final Deque<byte[]> batchMessages = new ArrayDeque<>();

        @Override
        public void onNext(final BarrageStreamGenerator.MessageView messageView) {
            try {
                messageView.forEachStream(inputStream -> {
                    try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                        inputStream.drainTo(baos);
                        batchMessages.add(baos.toByteArray());
                        inputStream.close();
                    } catch (final IOException e) {
                        throw new IllegalStateException("Failed to build barrage message: ", e);
                    }
                });
            } catch (final IOException e) {
                throw new IllegalStateException("Failed to generate barrage message: ", e);
            }
        }

        @Override
        public void onError(final Throwable throwable) {
            throw new IllegalStateException(throwable);
        }

        @Override
        public void onCompleted() {}
    }
}
