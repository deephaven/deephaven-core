//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.hotspot.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HotSpotImplTest {

    private HotSpotImpl SUT;

    @BeforeEach
    void setUp() {
        SUT = new HotSpotImpl();
    }

    @Test
    void getSafepointCount() {
        assertThat(SUT.getSafepointCount()).isNotNegative();
    }

    @Test
    void getTotalSafepointTimeMillis() {
        assertThat(SUT.getTotalSafepointTimeMillis()).isNotNegative();
    }

    @Test
    void getSafepointSyncTimeMillis() {
        assertThat(SUT.getSafepointSyncTimeMillis()).isNotNegative();
    }
}
