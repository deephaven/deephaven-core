package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TableSpecAdapter;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A table handle implements {@link io.deephaven.api.TableOperations} such that each the initial table handle and
 * derived table handles are managed as {@linkplain Export exports}.
 *
 * <p>
 * A table handle may only be combined with other table handles from the same {@linkplain Session session}.
 *
 * <p>
 * A table handle throws {@link UncheckedInterruptedException} and {@link UncheckedTableHandleException} on further
 * {@link io.deephaven.api.TableOperations}.
 *
 * @see TableHandleManager
 */
public final class TableHandle extends TableSpecAdapter<TableHandle, TableHandle>
        implements Closeable {

    public interface Lifecycle {
        void onInit(TableHandle handle);

        void onRelease(TableHandle handle);
    }

    /**
     * Create a table handle, exporting {@code table}. The table handle will be {@linkplain #isSuccessful() successful}
     * on return.
     *
     * @param session the session
     * @param table the table
     * @return the successful table handle
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TableHandleException if there is a table creation exception
     */
    public static TableHandle of(Session session, TableSpec table)
            throws InterruptedException, TableHandleException {
        return of(session, Collections.singletonList(table), null).get(0);
    }

    /**
     * Create a table handle, exporting {@code table}. The table handle will be {@linkplain #isSuccessful() successful}
     * on return. The given {@code lifecycle} will be called on initialization and on release. Derived table handles
     * will inherit the same {@code lifecycle}.
     *
     * @param session the session
     * @param table the table
     * @param lifecycle the lifecycle
     * @return the successful table handle
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TableHandleException if there is a table creation exception
     */
    public static TableHandle of(Session session, TableSpec table, Lifecycle lifecycle)
            throws InterruptedException, TableHandleException {
        return of(session, Collections.singletonList(table), lifecycle).get(0);
    }

    /**
     * Create the table handles, exporting {@code tables}. The table handles will be {@linkplain #isSuccessful()
     * successful} on return. The given {@code lifecycle} will be called on initialization and on release. Derived table
     * handles will inherit the same {@code lifecycle}.
     *
     * @param session the session
     * @param tables the tables
     * @param lifecycle the lifecycle
     * @return the successful table handles
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TableHandleException if there is a table creation exception
     */
    public static List<TableHandle> of(Session session, Iterable<TableSpec> tables,
            Lifecycle lifecycle) throws InterruptedException, TableHandleException {
        List<TableHandle> handles = impl(session, tables, lifecycle);
        for (TableHandle handle : handles) {
            handle.await();
            handle.throwOnError();
        }
        return handles;
    }

    static TableHandle ofUnchecked(Session session, TableSpec table, Lifecycle lifecycle) {
        final TableHandle handle = new TableHandle(table, lifecycle);
        List<Export> exports = session.export(ExportsRequest.of(handle.exportRequest()));
        if (exports.size() != 1) {
            throw new IllegalStateException();
        }
        handle.init(exports.get(0));
        handle.awaitUnchecked();
        handle.throwOnErrorUnchecked();
        return handle;
    }

    private static List<TableHandle> impl(Session session, Iterable<TableSpec> specs,
            Lifecycle lifecycle) {
        ExportsRequest.Builder exportBuilder = ExportsRequest.builder();
        List<TableHandle> handles = new ArrayList<>();
        for (TableSpec spec : specs) {
            TableHandle handle = new TableHandle(spec, lifecycle);
            handles.add(handle);
            exportBuilder.addRequests(handle.exportRequest());
        }
        ExportsRequest request = exportBuilder.build();
        List<Export> exports = session.export(request);
        if (exports.size() != handles.size()) {
            throw new IllegalStateException();
        }
        final int L = exports.size();
        for (int i = 0; i < L; ++i) {
            handles.get(i).init(exports.get(i));
        }
        return handles;
    }

    private final Lifecycle lifecycle;
    private Export export;

    private final CountDownLatch doneLatch;
    private ExportedTableCreationResponse response;
    private Throwable error;

    private TableHandle(TableSpec table, Lifecycle lifecycle) {
        super(table);
        this.lifecycle = lifecycle;
        this.doneLatch = new CountDownLatch(1);
    }

    public TableHandle newRef() {
        TableHandle handle = new TableHandle(table(), lifecycle);
        handle.init(export.newReference(new ResponseAdapter()));
        return handle;
    }

    public Export export() {
        return Objects.requireNonNull(export);
    }

    /**
     * Causes the current thread to wait until {@code this} {@linkplain #isDone() is done}, unless the thread is
     * {@linkplain Thread#interrupt interrupted}.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void await() throws InterruptedException {
        doneLatch.await();
    }

    public void awaitUnchecked() {
        try {
            await();
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e);
        }
    }

    /**
     * Causes the current thread to wait until {@code this} {@linkplain #isDone() is done}, unless the thread is
     * {@linkplain Thread#interrupt interrupted}, or {@code timeout} elapses.
     *
     * @param timeout the timeout
     * @return {@code true} if {@code this} has become {@linkplain #isDone() is done} and {@code false} if the
     *         {@code timeout} has elapsed
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean await(Duration timeout) throws InterruptedException {
        return doneLatch.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * The table proxy is done when the response from the server is done, which yields either a successful response or
     * an error.
     *
     * <p>
     * Note: the table proxy can create further derived table proxies before being done.
     *
     * @return {@code true} if {@code this} is done
     * @see #isSuccessful()
     * @see #error()
     */
    public boolean isDone() {
        return doneLatch.getCount() == 0;
    }

    /**
     * The table proxy is successful when the response from the server indicates success.
     *
     * @return {@code true} if the response from the server indicates success and {@code false} if the response
     *         indicates it was not successful or there has been no response yet
     * @see #isDone()
     */
    public boolean isSuccessful() {
        return response != null && response.getSuccess();
    }

    /**
     * The table handle has an error when the response from the server indicates an error.
     *
     * @return the error, if any
     * @see #isDone()
     * @see #isSuccessful()
     */
    public Optional<TableHandleException> error() {
        if (error != null) {
            return Optional.of(new TableHandleException(error));
        }
        if (response != null && !response.getSuccess()) {
            return Optional.of(new TableHandleException(response.getErrorInfo()));
        }
        return Optional.empty();
    }

    public void throwOnError() throws TableHandleException {
        final Optional<TableHandleException> error = error();
        if (error.isPresent()) {
            throw error.get();
        }
    }

    public void throwOnErrorUnchecked() {
        final Optional<TableHandleException> error = error();
        if (error.isPresent()) {
            throw error.get().asUnchecked();
        }
    }

    private ExportRequest exportRequest() {
        return ExportRequest.of(table(), new ResponseAdapter());
    }

    /**
     * Must be called after construction, before {@code this} is returned to the user.
     */
    private void init(Export export) {
        this.export = export;
        if (lifecycle != null) {
            lifecycle.onInit(this);
        }
    }

    @Override
    protected TableHandle adapt(TableSpec table) {
        return ofUnchecked(export.session(), table, lifecycle);
    }

    @Override
    protected TableSpec adapt(TableHandle rhs) {
        if (export.session() != rhs.export.session()) {
            throw new IllegalArgumentException("Can't mix multiple Sessions with TableHandle");
        }
        return rhs.export.table();
    }

    /**
     * Closes the underlying {@linkplain Export export}.
     */
    @Override
    public void close() {
        close(false);
    }

    void close(boolean skipNotify) {
        if (export.release()) {
            if (!skipNotify && lifecycle != null) {
                lifecycle.onRelease(this);
            }
        }
    }

    private class ResponseAdapter implements Listener {

        @Override
        public void onNext(ExportedTableCreationResponse response) {
            TableHandle.this.response = response;
            doneLatch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            TableHandle.this.error = t;
            doneLatch.countDown();
        }

        @Override
        public void onCompleted() {
            doneLatch.countDown();
        }
    }

    public final class TableHandleException extends Exception {

        public TableHandleException(String message) {
            super(message);
        }

        public TableHandleException(Throwable cause) {
            super(cause);
        }

        public UncheckedTableHandleException asUnchecked() {
            return new UncheckedTableHandleException(this);
        }

        public TableHandle handle() {
            return TableHandle.this;
        }

        public TableHandleException mixinStacktrace(StackTraceElement[] stackTrace) {
            final TableHandleException decoratedException = new TableHandleException(this);
            decoratedException.setStackTrace(stackTrace);
            return decoratedException;
        }
    }

    public final class UncheckedTableHandleException extends RuntimeException {

        private UncheckedTableHandleException(TableHandleException cause) {
            super(cause);
        }

        @Override
        public TableHandleException getCause() {
            return (TableHandleException) super.getCause();
        }

        public TableHandle handle() {
            return TableHandle.this;
        }
    }

    public final class UncheckedInterruptedException extends RuntimeException {

        private UncheckedInterruptedException(InterruptedException cause) {
            super(cause);
        }

        @Override
        public InterruptedException getCause() {
            return (InterruptedException) super.getCause();
        }

        public TableHandle handle() {
            return TableHandle.this;
        }
    }
}
