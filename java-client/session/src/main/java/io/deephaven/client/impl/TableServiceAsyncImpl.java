/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.client.impl.TableHandle.ResponseAdapter;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableServiceAsync.TableHandleAsync;
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

    static TableHandleAsync executeAsync(ExportService exportService, TableSpec tableSpec) {
        final TableHandleAsyncImpl impl = new TableHandleAsyncImpl(tableSpec);
        final ExportRequest request = ExportRequest.of(tableSpec, impl);
        final ExportServiceRequest esr = exportService.exportRequest(ExportsRequest.of(request));
        final List<Export> exports = esr.exports();
        if (exports.size() != 1) {
            throw new IllegalStateException();
        }
        impl.init(exports.get(0));
        esr.send();
        return impl;
    }

    static List<? extends TableHandleAsync> executeAsync(ExportService exportService, List<TableSpec> tableSpecs) {
        final int size = tableSpecs.size();
        final List<TableHandleAsyncImpl> impls = new ArrayList<>(size);
        final ExportsRequest.Builder builder = ExportsRequest.builder();
        for (TableSpec tableSpec : tableSpecs) {
            final TableHandleAsyncImpl impl = new TableHandleAsyncImpl(tableSpec);
            builder.addRequests(ExportRequest.of(tableSpec, impl));
            impls.add(impl);
        }
        final ExportServiceRequest esr = exportService.exportRequest(builder.build());
        final List<Export> exports = esr.exports();
        if (exports.size() != size) {
            throw new IllegalStateException();
        }
        for (int i = 0; i < size; ++i) {
            impls.get(i).init(exports.get(i));
        }
        esr.send();
        return impls;
    }

    private static class TableHandleAsyncImpl implements TableHandleAsync, Listener {
        private final TableSpec tableSpec;
        private final CompletableFuture<TableHandle> userFuture;
        private TableHandle handle;
        private Export export;

        TableHandleAsyncImpl(TableSpec tableSpec) {
            this.tableSpec = Objects.requireNonNull(tableSpec);
            this.userFuture = new CompletableFuture<>();
        }

        synchronized void init(Export export) {
            this.export = Objects.requireNonNull(export);
            this.userFuture.whenComplete((tableHandle, throwable) -> {
                if (throwable == null) {
                    // User now owns TableHandle, responsible for closing as appropriate
                    return;
                }
                if (isCancelled()) {
                    export.release();
                } else {
                    // When there is a "real" error, are we actually responsible for releasing the export?
                    // It _probably_ doesn't exist on the server.
                    // export.release();
                }
            });
            maybeComplete();
        }

        private void maybeComplete() {
            if (handle == null || export == null) {
                return;
            }
            handle.init(export);
            userFuture.complete(handle);
            handle = null;
            export = null;
        }

        // --------------------------

        @Override
        public void onNext(ExportedTableCreationResponse etcr) {
            final TableHandle handle = new TableHandle(tableSpec, null);
            final ResponseAdapter responseAdapter = handle.responseAdapter();
            responseAdapter.onNext(etcr);
            responseAdapter.onCompleted();
            final TableHandleException error = handle.error().orElse(null);
            if (error != null) {
                userFuture.completeExceptionally(error);
            } else {
                // It's possible that onNext comes before #init; either in the case where it was already cached from
                // io.deephaven.client.impl.ExportService.export, or where the RPC comes in asynchronously. In either
                // case, we need to store handle so it can potentially be completed here, or in init.
                synchronized (this) {
                    this.handle = handle;
                    maybeComplete();
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            userFuture.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!userFuture.isDone()) {
                userFuture.completeExceptionally(new IllegalStateException("onCompleted without future.isDone()"));
            }
        }

        // --------------------------

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return userFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return userFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return userFuture.isDone();
        }

        @Override
        public TableHandle get() throws InterruptedException, ExecutionException {
            return userFuture.get();
        }

        @Override
        public TableHandle get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            return userFuture.get(timeout, unit);
        }
    }
}
