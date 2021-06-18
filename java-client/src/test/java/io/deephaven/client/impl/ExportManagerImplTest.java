package io.deephaven.client.impl;

import static io.deephaven.client.impl.BatchTableRequestBuilder.byteStringToLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

import io.deephaven.qst.manager.ExportedTable;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.BatchTableRequest.Operation;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExportManagerImplTest {

    ExportManagerImplMock impl;

    @BeforeEach
    void setUp() {
        impl = new ExportManagerImplMock();
    }

    @Test
    void basicExportProperties() {
        final EmptyTable empty42 = Table.empty(42L);
        final ExportedTable exportedTable = impl.export(empty42);
        assertThat(exportedTable.table()).isEqualTo(empty42);
        assertThat(impl.batchTableRequests).hasSize(1);
        assertThat(impl.releasedTickets).isEmpty();
    }

    @Test
    void releaseIsCalled() {
        final EmptyTable empty42 = Table.empty(42L);
        impl.export(empty42).release();
        assertThat(impl.batchTableRequests).hasSize(1);
        assertThat(impl.releasedTickets).hasSize(1);
    }

    @Test
    void sameTicketOnSameExportedTable() {
        final EmptyTable empty42 = Table.empty(42L);
        try (final ExportedTableImpl ref1 = (ExportedTableImpl) impl.export(empty42);
            final ExportedTableImpl ref2 = (ExportedTableImpl) impl.export(empty42)) {
            assertThat(ref1.ticket()).isEqualTo(ref2.ticket());
        }
    }

    @Test
    void twoRefsForSameTable() {
        final EmptyTable empty42 = Table.empty(42L);

        final List<ExportedTable> refs = impl.export(Arrays.asList(empty42, empty42));
        assertThat(refs).hasSize(2);

        try (final ExportedTable ref1 = refs.get(0); final ExportedTable ref2 = refs.get(1)) {
            assertThat(ref1).isNotEqualTo(ref2);
        }
    }

    @Test
    void newTicketAfterRelease() {
        final EmptyTable empty42 = Table.empty(42L);
        final long ticket;
        try (final ExportedTableImpl ref = (ExportedTableImpl) impl.export(empty42)) {
            ticket = ref.ticket();
        }
        try (final ExportedTableImpl ref = (ExportedTableImpl) impl.export(empty42)) {
            assertThat(ref.ticket()).isNotEqualTo(ticket);
        }
    }

    @Test
    void newRefCanOutliveOriginal() {
        final EmptyTable empty42 = Table.empty(42L);
        final ExportedTableImpl newRef;
        try (final ExportedTableImpl ref = (ExportedTableImpl) impl.export(empty42)) {
            newRef = (ExportedTableImpl) ref.newRef();
        }
        assertThat(newRef.isReleased()).isFalse();
        newRef.release();
        assertThat(newRef.isReleased()).isTrue();
    }

    @Test
    void errorAfterRelease() {
        final EmptyTable empty42 = Table.empty(42L);
        final ExportedTable steal;
        try (final ExportedTable ref1 = impl.export(empty42)) {
            steal = ref1;
        }
        try {
            steal.newRef();
            failBecauseExceptionWasNotThrown(IllegalStateException.class);
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @Test
    void errorAfterReleaseEvenIfStillExported() {
        final EmptyTable empty42 = Table.empty(42L);
        try (final ExportedTable ref1 = impl.export(empty42)) {
            final ExportedTable steal;
            try (final ExportedTable ref2 = ref1.newRef()) {
                steal = ref2;
            }
            // ref1 is still alive here, but we can't use the stolen ref2 to do anything bad
            try {
                steal.newRef();
                failBecauseExceptionWasNotThrown(IllegalStateException.class);
            } catch (IllegalStateException e) {
                // expected
            }
        }
    }

    @Test
    void checkUnexportedParent() {
        final HeadTable empty42head6 = Table.empty(42L).head(6);
        final ExportedTable export = impl.export(empty42head6);
        assertThat(export.table()).isEqualTo(empty42head6);
        assertThat(impl.batchTableRequests).hasSize(1);
        assertThat(impl.batchTableRequests.get(0).getOpsList()).hasSize(2);
        assertThat(impl.batchTableRequests.get(0).getOpsList().get(0).hasEmptyTable()).isTrue();
        assertThat(impl.batchTableRequests.get(0).getOpsList().get(1).hasHead()).isTrue();
    }

    @Test
    void reusePreviousExports() {
        final EmptyTable empty42 = Table.empty(42L);
        final HeadTable empty42head6 = empty42.head(6);
        try (final ExportedTableImpl e1 = (ExportedTableImpl) impl.export(empty42);
            final ExportedTableImpl e2 = (ExportedTableImpl) impl.export(empty42head6)) {

            assertThat(impl.batchTableRequests).hasSize(2);
            // Check that we are re-using the ticket from e1
            assertThat(impl.batchTableRequests.get(1).getOpsList()).hasSize(1);
            assertThat(impl.batchTableRequests.get(1).getOps(0))
                .satisfies(op -> hasSourceId(op, e1.ticket()));
        }
    }

    @Test
    void mustReexportIfPreviousHasBeenReleased() {
        final EmptyTable empty42 = Table.empty(42L);
        final HeadTable empty42head6 = empty42.head(6);
        try (final ExportedTableImpl e1 = (ExportedTableImpl) impl.export(empty42)) {
            // ignore
        }
        try (final ExportedTableImpl e2 = (ExportedTableImpl) impl.export(empty42head6)) {
            assertThat(impl.batchTableRequests).hasSize(2);
            // Check that we aren't reusing the ticket from e1
            assertThat(impl.batchTableRequests.get(1).getOpsList()).hasSize(2);
        }
    }

    private static boolean hasSourceId(Operation op, long ticket) {
        final List<TableReference> references =
            ProtoHelper.getSourceIds(op).limit(2).collect(Collectors.toList());
        if (references.size() != 1) {
            return false;
        }
        final TableReference ref = references.get(0);
        if (!ref.hasTicket()) {
            return false;
        }
        return ticket == byteStringToLong(ref.getTicket().getId());
    }

    static class ExportManagerImplMock extends ExportManagerImpl {

        final List<BatchTableRequest> batchTableRequests = new ArrayList<>();
        final List<Ticket> releasedTickets = new ArrayList<>();

        @Override
        protected void execute(BatchTableRequest batchTableRequest) {
            batchTableRequests.add(batchTableRequest);
        }

        @Override
        protected void executeRelease(Ticket ticket) {
            releasedTickets.add(ticket);
        }
    }
}
