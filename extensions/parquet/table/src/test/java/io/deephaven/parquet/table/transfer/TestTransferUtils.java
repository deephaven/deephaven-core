//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.transfer;

import io.deephaven.util.QueryConstants;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static io.deephaven.parquet.table.transfer.TransferUtils.epochNanosUTC;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestTransferUtils {
    @Test
    void testEpochNanosUTC() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        AssertionsForClassTypes.assertThat(epochNanosUTC(ldt)).isEqualTo(nanos);
        assertThat(epochNanosUTC(null)).isEqualTo(QueryConstants.NULL_LONG);
    }
}
