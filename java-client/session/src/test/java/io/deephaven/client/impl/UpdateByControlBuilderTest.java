//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.proto.backplane.grpc.MathContext.RoundingMode;
import io.deephaven.proto.backplane.grpc.UpdateByRequest;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOptions;
import io.deephaven.proto.backplane.grpc.UpdateByRequest.UpdateByOptions;
import org.junit.jupiter.api.Test;

import java.math.MathContext;

import static org.assertj.core.api.Assertions.assertThat;

public class UpdateByControlBuilderTest {

    @Test
    void defaultInstance() {
        check(UpdateByControl.defaultInstance(), UpdateByOptions.getDefaultInstance());
    }

    @Test
    void useRedirection() {
        check(UpdateByControl.builder().useRedirection(false).build(),
                UpdateByOptions.newBuilder().setUseRedirection(false).build());
        check(UpdateByControl.builder().useRedirection(true).build(),
                UpdateByOptions.newBuilder().setUseRedirection(true).build());
    }

    @Test
    void chunkCapacity() {
        check(UpdateByControl.builder().chunkCapacity(42).build(),
                UpdateByOptions.newBuilder().setChunkCapacity(42).build());
    }

    @Test
    void maxStaticSparseMemoryOverhead() {
        check(UpdateByControl.builder().maxStaticSparseMemoryOverhead(1.42).build(),
                UpdateByOptions.newBuilder().setMaxStaticSparseMemoryOverhead(1.42).build());
    }

    @Test
    void initialHashTableSize() {
        check(UpdateByControl.builder().initialHashTableSize(13).build(),
                UpdateByOptions.newBuilder().setInitialHashTableSize(13).build());
    }

    @Test
    void maximumLoadFactor() {
        check(UpdateByControl.builder().maximumLoadFactor(0.99).build(),
                UpdateByOptions.newBuilder().setMaximumLoadFactor(0.99).build());
    }

    @Test
    void targetLoadFactor() {
        check(UpdateByControl.builder().targetLoadFactor(0.5).build(),
                UpdateByOptions.newBuilder().setTargetLoadFactor(0.5).build());
    }

    @Test
    void mathContext() {
        check(UpdateByControl.builder().mathContext(MathContext.DECIMAL32).build(),
                UpdateByOptions.newBuilder().setMathContext(io.deephaven.proto.backplane.grpc.MathContext.newBuilder()
                        .setPrecision(7).setRoundingMode(RoundingMode.HALF_EVEN).build()).build());
    }

    private static void check(UpdateByControl control, UpdateByOptions options) {
        assertThat(UpdateByBuilder.adapt(control)).isEqualTo(options);
    }
}
