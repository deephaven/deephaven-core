//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import io.deephaven.engine.util.TableTools;
import io.deephaven.proto.backplane.grpc.CrossJoinTablesRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.Status.Code;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A cross join request's reserve_bits of zero selects the default number of bits; any other value outside 1..62 is
 * rejected as an invalid argument before the join runs.
 */
public class CrossJoinGrpcTest extends GrpcTableOperationTestBase<CrossJoinTablesRequest> {

    @Override
    public ExportedTableCreationResponse send(final CrossJoinTablesRequest request) {
        return channel().tableBlocking().crossJoinTables(request);
    }

    private CrossJoinTablesRequest request(final int reserveBits) {
        return request(1, reserveBits);
    }

    private CrossJoinTablesRequest request(final int exportId, final int reserveBits) {
        final TableReference left = ref(TableTools.emptyTable(2).view("K = (int) ii", "A = (int) ii"));
        final TableReference right = ref(TableTools.emptyTable(2).view("K = (int) ii", "B = (int) ii"));
        return CrossJoinTablesRequest.newBuilder()
                .setResultId(ExportTicketHelper.wrapExportIdInTicket(exportId))
                .setLeftId(left)
                .setRightId(right)
                .addColumnsToMatch("K")
                .addColumnsToAdd("B")
                .setReserveBits(reserveBits)
                .build();
    }

    @Test
    public void zeroReserveBitsUsesDefault() {
        final ExportedTableCreationResponse response = send(request(0));
        assertThat(response.getSuccess()).isTrue();
        assertThat(response.getSize()).isEqualTo(2);
        release(response);
    }

    @Test
    public void reserveBitsInRangeAccepted() {
        int exportId = 1;
        for (final int reserveBits : new int[] {1, 62}) {
            final ExportedTableCreationResponse response = send(request(exportId++, reserveBits));
            assertThat(response.getSuccess()).isTrue();
            assertThat(response.getSize()).isEqualTo(2);
            release(response);
        }
    }

    @Test
    public void negativeReserveBitsRejected() {
        assertError(request(-1), Code.INVALID_ARGUMENT,
                "reserve_bits must be 0 (the default) or between 1 and 62 (inclusive), but was -1");
    }

    @Test
    public void tooLargeReserveBitsRejected() {
        assertError(request(63), Code.INVALID_ARGUMENT,
                "reserve_bits must be 0 (the default) or between 1 and 62 (inclusive), but was 63");
    }
}
