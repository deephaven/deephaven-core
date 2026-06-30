//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryWriterStateTest {

    @Test
    public void testFirstBatchUsesInsertionOrderAndStableIndexes() {
        final DictionaryWriterState state = new DictionaryWriterState(17);

        assertThat(state.hasDelta()).isTrue();
        assertThat(state.isFirstBatch()).isTrue();
        assertThat(state.indexFor("alpha")).isZero();
        assertThat(state.indexFor("beta")).isEqualTo(1);
        assertThat(state.indexFor("alpha")).isZero();

        assertThat(state.getDeltaValues()).containsExactly("alpha", "beta");
    }

    @Test
    public void testResetDeltaExposesOnlyNewValues() {
        final DictionaryWriterState state = new DictionaryWriterState(23);

        state.indexFor("alpha");
        state.indexFor("beta");
        state.resetDelta();

        assertThat(state.hasDelta()).isFalse();
        assertThat(state.isFirstBatch()).isFalse();
        assertThat(state.indexFor("beta")).isEqualTo(1);
        assertThat(state.hasDelta()).isFalse();

        assertThat(state.indexFor("gamma")).isEqualTo(2);
        assertThat(state.indexFor("delta")).isEqualTo(3);
        assertThat(state.getDeltaValues()).containsExactly("gamma", "delta");

        state.resetDelta();
        assertThat(state.hasDelta()).isFalse();
    }
}

