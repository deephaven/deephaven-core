package io.deephaven.client.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Pair;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PairBuilderTest {
    @Test
    void columnName() {
        check(ColumnName.of("Foo"),
                io.deephaven.proto.backplane.grpc.Pair.newBuilder().setOutputColumnName("Foo").build());
    }

    @Test
    void leftRight() {
        check(Pair.of(ColumnName.of("Input"), ColumnName.of("Output")), io.deephaven.proto.backplane.grpc.Pair
                .newBuilder().setOutputColumnName("Output").setInputColumnName("Input").build());
    }

    private static void check(Pair pair, io.deephaven.proto.backplane.grpc.Pair expected) {
        assertThat(PairBuilder.adapt(pair)).isEqualTo(expected);
    }
}
