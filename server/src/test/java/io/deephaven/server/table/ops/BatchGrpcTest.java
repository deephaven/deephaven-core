//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.testutil.testcase.FakeProcessEnvironment;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.runner.RecordingErrorTransformer;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.process.ProcessEnvironment;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchGrpcTest extends GrpcTableOperationTestBase<BatchTableRequest> {

    @Override
    public ExportedTableCreationResponse send(BatchTableRequest request) {
        throw new UnsupportedOperationException("batch returns stream");
    }

    @Test
    public void batchExample() {
        // empty_table(100).view("I=ii")
        final BatchTableRequest request = BatchTableRequest.newBuilder()
                .addOps(Operation.newBuilder()
                        .setEmptyTable(EmptyTableRequest.newBuilder()
                                .setSize(100)
                                .build())
                        .build())
                .addOps(Operation.newBuilder()
                        .setView(SelectOrUpdateRequest.newBuilder()
                                .setResultId(ExportTicketHelper.wrapExportIdInTicket(1))
                                .setSourceId(TableReference.newBuilder().setBatchOffset(0).build())
                                .addColumnSpecs("I=ii")
                                .build())
                        .build())
                .build();
        final Iterator<ExportedTableCreationResponse> it = channel().tableBlocking().batch(request);
        assertThat(it).hasNext();
        {
            final ExportedTableCreationResponse export = it.next();
            assertThat(export.getSuccess()).isTrue();
            assertThat(export.getIsStatic()).isTrue();
            assertThat(export.getSize()).isEqualTo(100);
        }
        assertThat(it).hasNext();
        {
            final ExportedTableCreationResponse export = it.next();
            assertThat(export.getSuccess()).isTrue();
            assertThat(export.getIsStatic()).isTrue();
            assertThat(export.getSize()).isEqualTo(100);
        }
        assertThat(it).isExhausted();
    }

    @Test
    public void empty() {
        final Iterator<ExportedTableCreationResponse> it =
                channel().tableBlocking().batch(BatchTableRequest.getDefaultInstance());
        try {
            it.next();
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
            assertThat(e.getMessage())
                    .contains("io.deephaven.proto.backplane.grpc.BatchTableRequest must have at least one ops (1)");
        }
    }

    @Test
    public void emptyOp() {
        final Iterator<ExportedTableCreationResponse> it =
                channel().tableBlocking().batch(BatchTableRequest.newBuilder()
                        .addOps(Operation.getDefaultInstance())
                        .build());
        try {
            it.next();
        } catch (StatusRuntimeException e) {
            assertThat(e.getStatus().getCode()).isEqualTo(Code.INVALID_ARGUMENT);
            assertThat(e.getMessage()).contains(
                    "io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation must have oneof op. Note: this may also indicate that the server is older than the client and doesn't know about this new oneof option.");
        }
    }

    /**
     * A batch export whose dependency was already released fails synchronously inside {@code submit()}, on the gRPC
     * thread, while this batch's query recorder is still running. That failure cascades to any dependent that an
     * earlier, still open batch registered out of order, and that batch's completion handler then runs on this thread.
     * The completion handler must not assume that it owns the thread's query recorder, and nothing a client does may
     * take the server down.
     */
    @Test
    public void dependencyReleasedBeforeDefinitionCascadesIntoEarlierBatch() throws Exception {
        final ProcessEnvironment previousEnvironment = ProcessEnvironment.tryGet();
        // a fatal error report must surface as an exception in this JVM rather than exit it
        ProcessEnvironment.set(FakeProcessEnvironment.INSTANCE, true);
        try {
            runDependencyReleasedBeforeDefinitionCascade();
        } finally {
            if (previousEnvironment == null) {
                ProcessEnvironment.clear();
            } else {
                ProcessEnvironment.set(previousEnvironment, true);
            }
        }
    }

    private void runDependencyReleasedBeforeDefinitionCascade() throws Exception {
        final int releasedId = 11;
        final int definedLaterId = 12;
        final int dependentId = 13;
        final Ticket released = ExportTicketHelper.wrapExportIdInTicket(releasedId);
        final Ticket definedLater = ExportTicketHelper.wrapExportIdInTicket(definedLaterId);
        final Ticket dependent = ExportTicketHelper.wrapExportIdInTicket(dependentId);

        // 1. create and release an export; later references to its ticket find a dead dependency
        {
            final Iterator<ExportedTableCreationResponse> it = channel().tableBlocking().batch(
                    BatchTableRequest.newBuilder()
                            .addOps(Operation.newBuilder()
                                    .setEmptyTable(EmptyTableRequest.newBuilder().setResultId(released).setSize(1)))
                            .build());
            assertThat(it.next().getSuccess()).isTrue();
            assertThat(it).isExhausted();
            release(released);
        }

        // 2. an earlier batch that depends, out of order, on a ticket nobody has defined yet; it stays open
        final List<ExportedTableCreationResponse> earlierResponses = new CopyOnWriteArrayList<>();
        final CompletableFuture<Void> earlierDone = new CompletableFuture<>();
        channel().table().batch(BatchTableRequest.newBuilder()
                .addOps(Operation.newBuilder()
                        .setView(SelectOrUpdateRequest.newBuilder()
                                .setResultId(dependent)
                                .setSourceId(TableReference.newBuilder().setTicket(definedLater))
                                .addColumnSpecs("J=ii")))
                .build(), new StreamObserver<>() {
                    @Override
                    public void onNext(final ExportedTableCreationResponse value) {
                        earlierResponses.add(value);
                    }

                    @Override
                    public void onError(final Throwable t) {
                        earlierDone.completeExceptionally(t);
                    }

                    @Override
                    public void onCompleted() {
                        earlierDone.complete(null);
                    }
                });
        final SessionState session = authenticatedSessionState();
        awaitCondition(() -> {
            final SessionState.ExportObject<?> export = session.getExportIfExists(dependentId);
            return export != null && export.getState() == ExportNotification.State.PENDING;
        });

        // 3. define the ticket from the released export; the definition fails inside submit() and cascades
        final List<ExportedTableCreationResponse> laterResponses = new ArrayList<>();
        try {
            channel().tableBlocking().batch(BatchTableRequest.newBuilder()
                    .addOps(Operation.newBuilder()
                            .setView(SelectOrUpdateRequest.newBuilder()
                                    .setResultId(definedLater)
                                    .setSourceId(TableReference.newBuilder().setTicket(released))
                                    .addColumnSpecs("K=ii")))
                    .build()).forEachRemaining(laterResponses::add);
        } catch (final StatusRuntimeException e) {
            throw new AssertionError("the defining batch failed as a whole instead of reporting one failed export;"
                    + " server-side errors: " + describeServerErrors(), e);
        }
        assertThat(laterResponses).hasSize(1);
        assertThat(laterResponses.get(0).getSuccess()).isFalse();

        earlierDone.get(10, TimeUnit.SECONDS);
        assertThat(earlierResponses).hasSize(1);
        assertThat(earlierResponses.get(0).getSuccess()).isFalse();
    }

    private String describeServerErrors() {
        final StringBuilder sb = new StringBuilder();
        for (final Throwable recorded : ((RecordingErrorTransformer) errorTransformer).getErrors()) {
            sb.append('\n');
            for (Throwable t = recorded; t != null; t = t.getCause()) {
                sb.append(t == recorded ? "  " : "\n    caused by: ").append(t);
            }
        }
        return sb.toString();
    }

    private static void awaitCondition(final BooleanSupplier condition) throws InterruptedException {
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("condition not met within 10 seconds");
            }
            Thread.sleep(5);
        }
    }
}
