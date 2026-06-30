//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryBatchStateManagerTest {

    @Test
    public void testGetOrCreateReusesStateForSameId() {
        final DictionaryBatchStateManager manager = new DictionaryBatchStateManager();

        final DictionaryBatchState first = manager.getOrCreate(7L, null);
        first.resetDelta();
        final DictionaryBatchState second = manager.getOrCreate(7L, null);

        assertThat(second).isSameAs(first);
        assertThat(manager.entries()).hasSize(1);
    }

    @Test
    public void testHasAnyDeltaReflectsManagedStates() {
        final DictionaryBatchStateManager manager = new DictionaryBatchStateManager();

        assertThat(manager.hasAnyDelta()).isFalse();

        final DictionaryBatchState first = manager.getOrCreate(1L, null);
        assertThat(manager.hasAnyDelta()).isTrue();

        first.resetDelta();
        assertThat(manager.hasAnyDelta()).isFalse();

        final DictionaryBatchState second = manager.getOrCreate(2L, null);
        second.resetDelta();
        assertThat(manager.hasAnyDelta()).isFalse();

        second.indexFor("new-value");
        assertThat(manager.hasAnyDelta()).isTrue();
    }
}

