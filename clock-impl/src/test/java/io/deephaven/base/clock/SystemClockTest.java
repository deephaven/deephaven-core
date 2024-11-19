//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.clock;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;

class SystemClockTest {
    @Test
    void of() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {
        assertThat(SystemClock.of()).isExactlyInstanceOf(SystemClockJdkInternalMiscVm.class);
    }

    @Test
    void serviceLoader() {
        assertThat(SystemClock.serviceLoader()).get().isExactlyInstanceOf(SystemClockJdkInternalMiscVm.class);
    }
}
