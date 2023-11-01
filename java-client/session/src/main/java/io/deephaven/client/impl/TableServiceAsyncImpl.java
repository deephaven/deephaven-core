/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.client.impl.TableHandle.ResponseAdapter;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.client.impl.TableServiceAsync.TableHandleFuture;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.qst.table.TableSpec;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
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
            // We would like to be able to proactively release an export when the user cancels a future. There are a
            // couple reasons why we can't currently do this:
            //
            // 1. The release RPC only works with exports that have already been created. It's possible that a release
            // RPC would race w/ the batch RPC. And even if we guarantee the release comes on the wire after the batch,
            // it's still possible the batch impl hasn't created the exports yet. In either case, a race leads to a
            // leaked export.
            //
            // 2. The release RPC is non-deterministic with how it handles releases when the export _does_ exist: if the
            // export is still in process, it transitions the export to a CANCELLED state, and then propagates cancels
            // to downstream dependencies; if the export is already EXPORTED (ie, finished initial computation), then
            // the state transitions to RELEASED which does _not_ cancel downstream dependencies. This bifurcating
            // behavior is strange - it seems like typical liveness abstractions should be used instead.
            //
            // [future_1, future_2] = executeAsync([table_spec_1, table_spec_2]);
            // future_1.cancel(true);
            //
            // In the pseudocode above, I maintain that a) we should be able to immediately tell the server we no longer
            // require an export for table_spec_1, and b) future_2 should still be valid, regardless of whether it
            // depends on table_spec_1 or not.
            //
            // See io.deephaven.server.session.SessionState.ExportObject.cancel.
            //
            // We _could_ work around the cancel propagation issue by doing an isolated FetchTableRequest for each
            // export the user requests, but regardless that doesn't solve the former issue.
            //
            // TODO: We need to make sure Batch exports get a chance to establish all dependencies wrt liveness before
            // race w/ release.
            //
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
            handle.mitigateDhc4754(Duration.ofSeconds(1));
            if (!future.complete(handle)) {
                // If we are unable to complete the future, it means the user cancelled it. It's only at this point in
                // time we are able to let the server know that we don't need it anymore. See comments in #init.
                handle.close();
            }
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
                future.completeExceptionally(error);
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
