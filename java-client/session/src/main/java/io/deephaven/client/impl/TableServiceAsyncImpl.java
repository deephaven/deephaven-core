/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.client.impl.TableHandle.ResponseAdapter;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableService.TableHandleFuture;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.qst.table.TableSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class TableServiceAsyncImpl {

    static TableHandleFuture executeAsync(ExportService exportService, TableSpec tableSpec) {
        final TableHandleAsyncImpl impl = new TableHandleAsyncImpl(tableSpec);
        final ExportRequest request = ExportRequest.of(tableSpec, impl);
        try (final ExportServiceRequest esr = exportService.exportRequest(ExportsRequest.of(request))) {
            final List<Export> exports = esr.exports();
            if (exports.size() != 1) {
                throw new IllegalStateException();
            }
            impl.init(exports.get(0));
            esr.send();
        }
        return impl;
    }

    static List<? extends TableHandleFuture> executeAsync(ExportService exportService,
            Iterable<? extends TableSpec> tableSpecs) {
        final List<TableHandleAsyncImpl> impls = new ArrayList<>();
        final ExportsRequest.Builder builder = ExportsRequest.builder();
        for (TableSpec tableSpec : tableSpecs) {
            final TableHandleAsyncImpl impl = new TableHandleAsyncImpl(tableSpec);
            builder.addRequests(ExportRequest.of(tableSpec, impl));
            impls.add(impl);
        }
        final int size = impls.size();
        try (final ExportServiceRequest esr = exportService.exportRequest(builder.build())) {
            final List<Export> exports = esr.exports();
            if (exports.size() != size) {
                throw new IllegalStateException();
            }
            for (int i = 0; i < size; ++i) {
                impls.get(i).init(exports.get(i));
            }
            esr.send();
        }
        return impls;
    }

    private static class TableHandleAsyncImpl implements TableHandleFuture, Listener {
        private final TableSpec tableSpec;
        private final CompletableFuture<TableHandle> future;
        private TableHandle handle;
        private Export export;

        TableHandleAsyncImpl(TableSpec tableSpec) {
            this.tableSpec = Objects.requireNonNull(tableSpec);
            this.future = new CompletableFuture<>();
        }

        synchronized void init(Export export) {
            this.export = Objects.requireNonNull(export);
            // TODO(deephaven-core#4781): Immediately notify server of release when user cancels TableHandleFuture
            // this.future.whenComplete((tableHandle, throwable) -> {
            // if (isCancelled()) {
            // export.release();
            // }
            // });
            maybeComplete();
        }

        private void maybeComplete() {
            if (handle == null || export == null) {
                return;
            }
            handle.init(export);
            if (!future.complete(handle)) {
                // If we are unable to complete the future, it means the user cancelled it. It's only at this point in
                // time we are able to let the server know that we don't need it anymore.
                // TODO(deephaven-core#4781): Immediately notify server of release when user cancels TableHandleFuture
                handle.close();
            }
            handle = null;
            export = null;
        }

        // --------------------------

        @Override
        public void onNext(ExportedTableCreationResponse etcr) {
            final TableHandle tableHandle = new TableHandle(tableSpec, null);
            final ResponseAdapter responseAdapter = tableHandle.responseAdapter();
            responseAdapter.onNext(etcr);
            responseAdapter.onCompleted();
            final TableHandleException error = tableHandle.error().orElse(null);
            if (error != null) {
                future.completeExceptionally(error);
            } else {
                // It's possible that onNext comes before #init; either in the case where it was already cached from
                // io.deephaven.client.impl.ExportService.export, or where the RPC comes in asynchronously. In either
                // case, we need to store handle so it can potentially be completed here, or in init.
                synchronized (this) {
                    handle = tableHandle;
                    maybeComplete();
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(new IllegalStateException("onCompleted without future.isDone()"));
            }
        }

        // --------------------------

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public TableHandle get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public TableHandle get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }
    }
}
