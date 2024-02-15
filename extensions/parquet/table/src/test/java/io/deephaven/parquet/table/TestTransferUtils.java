/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table;

import io.deephaven.parquet.table.util.TransferUtils;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

final public class TestTransferUtils {

    @Test
    public void testEpochNanosUTC() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(nanos, TransferUtils.epochNanosUTC(ldt));
        TestCase.assertEquals(QueryConstants.NULL_LONG, TransferUtils.epochNanosUTC(null));
    }

    @Test
    public void testEpochNanosTo() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(ldt, TransferUtils.epochNanosToLocalDateTimeUTC(nanos));
        TestCase.assertNull(TransferUtils.epochNanosToLocalDateTimeUTC(QueryConstants.NULL_LONG));
    }

    @Test
    public void testEpochMicrosTo() {
        long nanos = 123456789123456789L;
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        nanos = DateTimeUtils.microsToNanos(micros);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(ldt, TransferUtils.epochMicrosToLocalDateTimeUTC(micros));
        TestCase.assertNull(TransferUtils.epochMicrosToLocalDateTimeUTC(QueryConstants.NULL_LONG));
    }

    @Test
    public void testEpochMillisTo() {
        long nanos = 123456789123456789L;
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        nanos = DateTimeUtils.millisToNanos(millis);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(ldt, TransferUtils.epochMillisToLocalDateTimeUTC(millis));
        TestCase.assertNull(TransferUtils.epochMillisToLocalDateTimeUTC(QueryConstants.NULL_LONG));
    }
}
