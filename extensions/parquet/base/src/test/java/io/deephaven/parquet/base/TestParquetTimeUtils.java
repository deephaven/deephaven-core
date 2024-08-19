//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TestParquetTimeUtils {

    @Test
    public void testEpochNanosUTC() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(nanos, ParquetTimeUtils.epochNanosUTC(ldt));
        TestCase.assertEquals(QueryConstants.NULL_LONG, ParquetTimeUtils.epochNanosUTC(null));
    }

    @Test
    public void testEpochNanosTo() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(ldt, ParquetTimeUtils.epochNanosToLocalDateTimeUTC(nanos));
        TestCase.assertNull(ParquetTimeUtils.epochNanosToLocalDateTimeUTC(QueryConstants.NULL_LONG));
    }

    @Test
    public void testEpochMicrosTo() {
        long nanos = 123456789123456789L;
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        nanos = DateTimeUtils.microsToNanos(micros);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(ldt, ParquetTimeUtils.epochMicrosToLocalDateTimeUTC(micros));
        TestCase.assertNull(ParquetTimeUtils.epochMicrosToLocalDateTimeUTC(QueryConstants.NULL_LONG));
    }

    @Test
    public void testEpochMillisTo() {
        long nanos = 123456789123456789L;
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        nanos = DateTimeUtils.millisToNanos(millis);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        TestCase.assertEquals(ldt, ParquetTimeUtils.epochMillisToLocalDateTimeUTC(millis));
        TestCase.assertNull(ParquetTimeUtils.epochMillisToLocalDateTimeUTC(QueryConstants.NULL_LONG));
    }
}
