//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.hotspot;

import io.deephaven.hotspot.impl.HotSpotImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class HotSpotTest {

    @Test
    void loadImpl() {
        assertThat(HotSpot.loadImpl()).get().isExactlyInstanceOf(HotSpotImpl.class);
    }
}
