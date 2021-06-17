package io.deephaven.client.impl;

import static org.assertj.core.api.Assertions.assertThat;

import io.deephaven.client.ExportedTable;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.HeadTable;
import io.deephaven.qst.table.Table;
import java.util.ArrayList;
import java.util.List;
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
        assertThat(exportedTable.manager()).isEqualTo(impl);
        assertThat(exportedTable.table()).isEqualTo(empty42);
        assertThat(impl.batchTableRequests).hasSize(1);
        assertThat(impl.releasedTickets).isEmpty();
    }

    @Test
    void sameExportedTable() {
        final EmptyTable empty42 = Table.empty(42L);
        final ExportedTable export1 = impl.export(empty42);
        final ExportedTable export2 = impl.export(empty42);
        assertThat(export1).isEqualTo(export2);
        assertThat(impl.batchTableRequests).hasSize(1);
        assertThat(impl.releasedTickets).isEmpty();
    }

    @Test
    void notSameAfterRelease() {
        final EmptyTable empty42 = Table.empty(42L);
        final ExportedTable export1 = impl.export(empty42);
        impl.release(export1);
        final ExportedTable export2 = impl.export(empty42);
        assertThat(export1).isNotEqualTo(export2);
        assertThat(impl.batchTableRequests).hasSize(2);
        assertThat(impl.releasedTickets).hasSize(1);
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
