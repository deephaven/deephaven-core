//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AggregationOptionsTest {

    @Test
    void defaults() {
        final AggregationOptions<String> options = AggregationOptions.defaults();
        assertThat(options.preserveEmpty()).isFalse();
        assertThat(options.initialGroups()).isEmpty();
        assertThat(options.stateReclamation()).isEqualTo(StateReclamation.configured());
    }

    @Test
    void builder() {
        final AggregationOptions<String> options = AggregationOptions.<String>builder()
                .preserveEmpty(true)
                .initialGroups("groups")
                .stateReclamation(StateReclamation.none())
                .build();
        assertThat(options.preserveEmpty()).isTrue();
        assertThat(options.initialGroups()).hasValue("groups");
        assertThat(options.stateReclamation()).isEqualTo(StateReclamation.none());
    }

    @Test
    void mapInitialGroups() {
        final AggregationOptions<String> options = AggregationOptions.<String>builder()
                .preserveEmpty(true)
                .initialGroups("groups")
                .stateReclamation(StateReclamation.releaseBlocks(0.5))
                .build();
        final AggregationOptions<Integer> mapped = options.mapInitialGroups(String::length);
        assertThat(mapped.preserveEmpty()).isTrue();
        assertThat(mapped.initialGroups()).hasValue(6);
        assertThat(mapped.stateReclamation()).isEqualTo(StateReclamation.releaseBlocks(0.5));
    }

    @Test
    void mapWithoutInitialGroupsNeverCallsTheMapper() {
        final AggregationOptions<String> options = AggregationOptions.defaults();
        final AggregationOptions<Integer> mapped = options.mapInitialGroups(group -> {
            throw new AssertionError("no initial groups to map");
        });
        assertThat(mapped).isEqualTo(AggregationOptions.<Integer>defaults());
    }

    @Test
    void stateReclamationKinds() {
        assertThat(StateReclamation.configured().kind()).isEqualTo(StateReclamation.Kind.CONFIGURED);
        assertThat(StateReclamation.none().kind()).isEqualTo(StateReclamation.Kind.NONE);
        assertThat(StateReclamation.releaseBlocks()).isEqualTo(StateReclamation.releaseBlocks(1.0));
        assertThat(StateReclamation.releaseBlocks(0.25).collapseFreeFraction()).isEqualTo(0.25);
    }

    @Test
    void stateReclamationClampsFraction() {
        assertThat(StateReclamation.releaseBlocks(-1).collapseFreeFraction()).isEqualTo(0.0);
        assertThat(StateReclamation.releaseBlocks(2).collapseFreeFraction()).isEqualTo(1.0);
    }

    @Test
    void stateReclamationRejectsNaN() {
        assertThatThrownBy(() -> StateReclamation.releaseBlocks(Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stateReclamationRejectsAFractionForOtherKinds() {
        assertThatThrownBy(() -> ImmutableStateReclamation.of(StateReclamation.Kind.NONE, 0.5))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
