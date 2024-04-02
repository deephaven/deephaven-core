//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.table.TableSpec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class TableServiceImpl {
    /**
     * Create a table handle, exporting {@code table}. The table handle will be {@link TableHandle#isSuccessful()
     * successful} on return. The given {@code lifecycle} will be called on initialization and on release. Derived table
     * handles will inherit the same {@code lifecycle}.
     *
     * @param exportService the export service
     * @param table the table
     * @param lifecycle the lifecycle
     * @return the successful table handle
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TableHandleException if there is a table creation exception
     */
    static TableHandle execute(ExportService exportService, TableSpec table, Lifecycle lifecycle)
            throws InterruptedException, TableHandleException {
        return execute(exportService, Collections.singletonList(table), lifecycle).get(0);
    }

    /**
     * Create the table handles, exporting {@code tables}. The table handles will be {@link TableHandle#isSuccessful()
     * successful} on return. The given {@code lifecycle} will be called on initialization and on release. Derived table
     * handles will inherit the same {@code lifecycle}.
     *
     * @param exportService the export service
     * @param tables the tables
     * @param lifecycle the lifecycle
     * @return the successful table handles
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TableHandleException if there is a table creation exception
     */
    static List<TableHandle> execute(ExportService exportService, Iterable<TableSpec> tables, Lifecycle lifecycle)
            throws InterruptedException, TableHandleException {
        List<TableHandle> handles = executeImpl(exportService, tables, lifecycle);
        for (TableHandle handle : handles) {
            handle.await();
            handle.throwOnError();
        }
        return handles;
    }

    static TableHandle executeUnchecked(ExportService exportService, TableSpec table, Lifecycle lifecycle) {
        final TableHandle handle = new TableHandle(table, lifecycle);
        try (final ExportServiceRequest request =
                exportService.exportRequest(ExportsRequest.of(handle.exportRequest()))) {
            List<Export> exports = request.exports();
            if (exports.size() != 1) {
                throw new IllegalStateException();
            }
            handle.init(exports.get(0));
            request.send();
        }
        handle.awaitUnchecked();
        handle.throwOnErrorUnchecked();
        return handle;
    }

    private static List<TableHandle> executeImpl(ExportService exportService, Iterable<TableSpec> specs,
            Lifecycle lifecycle) {
        ExportsRequest.Builder exportBuilder = ExportsRequest.builder();
        List<TableHandle> handles = new ArrayList<>();
        for (TableSpec spec : specs) {
            TableHandle handle = new TableHandle(spec, lifecycle);
            handles.add(handle);
            exportBuilder.addRequests(handle.exportRequest());
        }
        try (final ExportServiceRequest request = exportService.exportRequest(exportBuilder.build())) {
            final List<Export> exports = request.exports();
            if (exports.size() != handles.size()) {
                throw new IllegalStateException();
            }
            final int L = exports.size();
            for (int i = 0; i < L; ++i) {
                handles.get(i).init(exports.get(i));
            }
            request.send();
        }
        return handles;
    }

    interface Lifecycle {
        void onInit(TableHandle handle);

        void onRelease(TableHandle handle);
    }
}
