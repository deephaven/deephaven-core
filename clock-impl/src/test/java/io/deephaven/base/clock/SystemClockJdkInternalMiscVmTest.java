//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.clock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class SystemClockJdkInternalMiscVmTest {

    private SystemClockJdkInternalMiscVm SUT;

    @BeforeEach
    void setUp() {
        SUT = new SystemClockJdkInternalMiscVm();
    }

    @Test
    void currentTimeMillis() {
        assertThat(SUT.currentTimeMillis()).isPositive();
    }

    @Test
    void currentTimeMicros() {
        assertThat(SUT.currentTimeMicros()).isPositive();
    }

    @Test
    void currentTimeNanos() {
        assertThat(SUT.currentTimeNanos()).isPositive();
    }

    @Test
    void instantMillis() {
        assertThat(SUT.instantMillis()).isAfter(Instant.EPOCH);
    }

    @Test
    void instantNanos() {
        assertThat(SUT.instantNanos()).isAfter(Instant.EPOCH);
    }
}
