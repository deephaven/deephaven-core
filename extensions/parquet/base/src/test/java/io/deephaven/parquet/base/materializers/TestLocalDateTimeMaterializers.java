//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.time.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestLocalDateTimeMaterializers {
    @Test
    void testEpochNanosTo() {
        final long nanos = 123456789123456789L;
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        assertThat(LocalDateTimeFromNanosMaterializer.convertValue(nanos)).isEqualTo(ldt);
    }

    @Test
    void testEpochMicrosTo() {
        long nanos = 123456789123456789L;
        final long micros = DateTimeUtils.nanosToMicros(nanos);
        nanos = DateTimeUtils.microsToNanos(micros);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        assertThat(LocalDateTimeFromMicrosMaterializer.convertValue(micros)).isEqualTo(ldt);
    }

    @Test
    void testEpochMillisTo() {
        long nanos = 123456789123456789L;
        final long millis = DateTimeUtils.nanosToMillis(nanos);
        nanos = DateTimeUtils.millisToNanos(millis);
        final Instant dt2 = Instant.ofEpochSecond(0, nanos);
        final LocalDateTime ldt = LocalDateTime.ofInstant(dt2, ZoneId.of("UTC"));
        assertThat(LocalDateTimeFromMillisMaterializer.convertValue(millis)).isEqualTo(ldt);
    }
}
