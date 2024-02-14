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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

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
        private final CompletableFuture<Export> exportFuture;
        private final CompletableFuture<ExportedTableCreationResponse> etcrFuture;
        private final CompletableFuture<TableHandle> future;

        TableHandleAsyncImpl(TableSpec tableSpec) {
            this.tableSpec = Objects.requireNonNull(tableSpec);
            exportFuture = new CompletableFuture<>();
            etcrFuture = new CompletableFuture<>();
            final CompletableFuture<TableHandle> internalFuture = CompletableFuture
                    .allOf(exportFuture, etcrFuture)
                    .thenCompose(this::complete);
            // thenApply(Function.identity()) _may_ seem extraneous, but we need to ensure separation between the user's
            // future and our internal state
            future = internalFuture.thenApply(Function.identity());
            future.whenComplete((tableHandle, throwable) -> {
                // TODO(deephaven-core#4781): Immediately notify server of release when user cancels TableHandleFuture
                if (throwable instanceof CancellationException) {
                    // Would be better if we could immediately tell server of release, but currently we need to wait for
                    // etcr/export object.
                    internalFuture.thenAccept(TableHandle::close);
                }
            });
        }

        void init(Export export) {
            // Note: we aren't expecting exceptional completions of exportFuture; we're using a future to make it easy
            // to compose with our etcrFuture (which may or may not be completed before exportFuture).
            // In exceptional cases where we _don't_ complete exportFuture (for example, the calling code has a runtime
            // exception), we know we _haven't_ called io.deephaven.client.impl.ExportServiceRequest#send, so there
            // isn't any possibility that we have left open a server-side export. And in those cases, this object isn't
            // returned to the user and becomes garbage. The client-side cleanup will be handled in
            // io.deephaven.client.impl.ExportServiceRequest#cleanupUnsent.
            exportFuture.complete(Objects.requireNonNull(export));
        }

        // --------------------------

        private CompletionStage<TableHandle> complete(Void ignore) {
            final Export export = Objects.requireNonNull(exportFuture.getNow(null));
            final ExportedTableCreationResponse etcr = Objects.requireNonNull(etcrFuture.getNow(null));
            final TableHandle tableHandle = new TableHandle(tableSpec, null);
            tableHandle.init(export);
            final ResponseAdapter responseAdapter = tableHandle.responseAdapter();
            responseAdapter.onNext(etcr);
            responseAdapter.onCompleted();
            final TableHandleException error = tableHandle.error().orElse(null);
            if (error != null) {
                // Only available in Java 9+
                // return CompletableFuture.failedStage(error);
                final CompletableFuture<TableHandle> f = new CompletableFuture<>();
                f.completeExceptionally(error);
                return f;
            }
            // Only available in Java 9+
            // return CompletableFuture.completedStage(tableHandle);
            return CompletableFuture.completedFuture(tableHandle);
        }

        // --------------------------

        @Override
        public void onNext(ExportedTableCreationResponse etcr) {
            etcrFuture.complete(etcr);
        }

        @Override
        public void onError(Throwable t) {
            etcrFuture.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!etcrFuture.isDone()) {
                etcrFuture.completeExceptionally(new IllegalStateException("onCompleted without etcrFuture.isDone()"));
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
