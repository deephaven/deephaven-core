package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.grpc_api.DeephavenChannel;
import io.deephaven.grpc_api.util.OperationHelper;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceImplBase;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceImplBase;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.TableSpec;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

@RunWith(JUnit4.class)
public class ExportStatesTest {

    @Rule
    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private final List<Ticket> releases = new ArrayList<>();
    private final List<BatchTableRequest> batches = new ArrayList<>();

    SessionServiceImplBase session = new SessionServiceGrpc.SessionServiceImplBase() {

        @Override
        public void release(ReleaseRequest request, StreamObserver<ReleaseResponse> responseObserver) {
            releases.add(request.getId());
            responseObserver.onNext(ReleaseResponse.getDefaultInstance());
            responseObserver.onCompleted();
        }
    };

    TableServiceImplBase table = new TableServiceGrpc.TableServiceImplBase() {
        @Override
        public void batch(BatchTableRequest request,
                StreamObserver<ExportedTableCreationResponse> responseObserver) {
            batches.add(request);
            int ix = 0;
            for (Operation operation : request.getOpsList()) {
                ExportedTableCreationResponse response = ExportedTableCreationResponse.newBuilder()
                        .setSuccess(true)
                        .setResultId(TableReference.newBuilder().setBatchOffset(ix).build()).build();
                responseObserver.onNext(response);
                ++ix;
            }
            responseObserver.onCompleted();
        }
    };

    ExportStates states;

    @Before
    public void setUp() throws IOException {

        String serverName = InProcessServerBuilder.generateName();

        grpcCleanup.register(InProcessServerBuilder.forName(serverName).directExecutor()
                .addService(session).addService(table).build().start());

        ManagedChannel channel = grpcCleanup
                .register(InProcessChannelBuilder.forName(serverName).directExecutor().build());

        DeephavenChannel deephavenChannel = new DeephavenChannel(channel);

        states = new ExportStates(deephavenChannel.session(), deephavenChannel.table(), new ExportTicketCreator());
    }

    Export export(TableSpec table) {
        return states.export(ExportsRequest.logging(table)).get(0);
    }

    List<Export> export(TableSpec... tables) {
        return states.export(ExportsRequest.logging(tables));
    }

    @Test
    public void basicExportProperties() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        final Export Export = export(empty42);
        assertThat(Export.table()).isEqualTo(empty42);
        verifyBatches(1);
        verifyReleases(0);
    }

    @Test
    public void releaseIsCalled() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        export(empty42).release();
        verifyBatches(1);
        verifyReleases(1);
    }

    @Test
    public void sameTicketOnSameExport() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        try (final Export ref1 = export(empty42); final Export ref2 = export(empty42)) {
            assertThat(ref1.ticket()).isEqualTo(ref2.ticket());
        }
    }

    @Test
    public void twoRefsForSameTable() {
        final EmptyTable empty42 = TableSpec.empty(42L);

        final List<Export> refs = export(empty42, empty42);
        assertThat(refs).hasSize(2);

        try (final Export ref1 = refs.get(0); final Export ref2 = refs.get(1)) {
            assertThat(ref1).isNotEqualTo(ref2);
        }
    }

    @Test
    public void newTicketAfterRelease() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        final Ticket ticket;
        try (final Export ref = export(empty42)) {
            ticket = ref.ticket();
        }
        try (final Export ref = export(empty42)) {
            assertThat(ref.ticket()).isNotEqualTo(ticket);
        }
    }

    @Test
    public void newRefCanOutliveOriginal() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        final Export newRef;
        try (final Export ref = export(empty42)) {
            newRef = ref.newReference(Listener.logging());
        }
        assertThat(newRef.isReleased()).isFalse();
        newRef.release();
        assertThat(newRef.isReleased()).isTrue();
    }

    @Test
    public void errorAfterRelease() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        final Export steal;
        try (final Export ref1 = export(empty42)) {
            steal = ref1;
        }
        try {
            steal.newReference(Listener.logging());
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    public void errorAfterReleaseEvenIfStillExported() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        try (final Export ref1 = export(empty42)) {
            final Export steal;
            try (final Export ref2 = ref1.newReference(Listener.logging())) {
                steal = ref2;
            }
            // ref1 is still alive here, but we can't use the stolen ref2 to do anything bad
            try {
                steal.newReference(Listener.logging());
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                // expected
            }
        }
    }

    @Test
    public void checkUnexportedParent() {
        final HeadTable empty42head6 = TableSpec.empty(42L).head(6);
        final Export export = export(empty42head6);
        assertThat(export.table()).isEqualTo(empty42head6);

        assertThat(batches).hasSize(1);
        assertThat(batches.get(0).getOpsList()).hasSize(2);
        assertThat(batches.get(0).getOpsList().get(0).hasEmptyTable()).isTrue();
        assertThat(batches.get(0).getOpsList().get(1).hasHead()).isTrue();
    }

    @Test
    public void reusePreviousExports() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        final HeadTable empty42head6 = empty42.head(6);
        try (final Export e1 = export(empty42); final Export e2 = export(empty42head6)) {
            assertThat(batches).hasSize(2); // Check that we are re-using the
            // ticket from e1 assertThat(batches.get(1).getOpsList()).hasSize(1);
            assertThat(batches.get(1).getOps(0)).satisfies(op -> hasSourceId(op, e1.ticket()));
        }
    }

    @Test
    public void mustReexportIfPreviousHasBeenReleased() {
        final EmptyTable empty42 = TableSpec.empty(42L);
        final HeadTable empty42head6 = empty42.head(6);
        try (final Export e1 = export(empty42)) {
            // ignore
        }
        try (final Export e2 = export(empty42head6)) {
            assertThat(batches).hasSize(2);
            // Check that we aren't reusing the ticket from e1
            assertThat(batches.get(1).getOpsList()).hasSize(2);
        }
    }

    private void verifyBatches(int size) {
        assertThat(batches).hasSize(size);
    }

    private void verifyReleases(int size) {
        assertThat(releases).hasSize(size);
    }

    private static boolean hasSourceId(Operation op, Ticket ticket) {
        final List<TableReference> references =
                OperationHelper.getSourceIds(op).limit(2).collect(Collectors.toList());
        if (references.size() != 1) {
            return false;
        }
        final TableReference ref = references.get(0);
        if (!ref.hasTicket()) {
            return false;
        }
        return ticket.equals(ref.getTicket());
    }
}
