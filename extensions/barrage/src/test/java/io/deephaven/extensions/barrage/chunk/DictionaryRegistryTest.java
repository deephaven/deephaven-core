//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DictionaryRegistryTest {

    private DictionaryRegistry registry;
    private WritableObjectChunk<Object, Values> chunk;

    @Before
    public void setup() {
        registry = new DictionaryRegistry();
    }

    @After
    public void teardown() {
        if (chunk != null) {
            chunk.close();
        }
    }

    @Test
    public void testDenseSequentialDictionaries() {
        for (int dictId = 0; dictId < 3; dictId++) {
            chunk = makeTestChunk(new Object[] {"val_" + dictId});
            registry.update(dictId, chunk, false);
            chunk.close();
            chunk = null;
        }

        assertThat(registry.get(0L)).containsExactly("val_0");
        assertThat(registry.get(1L)).containsExactly("val_1");
        assertThat(registry.get(2L)).containsExactly("val_2");
    }

    @Test
    public void testDeltaAppend() {
        // First batch: full replacement
        chunk = makeTestChunk(new Object[] {"a", "b"});
        registry.update(5L, chunk, false);
        chunk.close();

        assertThat(registry.get(5L)).containsExactly("a", "b");

        // Second batch: delta append
        chunk = makeTestChunk(new Object[] {"c", "d"});
        registry.update(5L, chunk, true);
        chunk.close();

        assertThat(registry.get(5L)).containsExactly("a", "b", "c", "d");
    }

    @Test
    public void testSparseIdsWithArrayGrowth() {
        // Fire at ID 100, should grow array from initial 16
        chunk = makeTestChunk(new Object[] {"sparse"});
        registry.update(100L, chunk, false);
        chunk.close();

        assertThat(registry.get(100L)).containsExactly("sparse");
        assertThat(registry.get(0L)).isNull();
        assertThat(registry.get(50L)).isNull();
    }

    @Test
    public void testNegativeIdThrows() {
        chunk = makeTestChunk(new Object[] {"val"});
        assertThatThrownBy(() -> registry.update(-1L, chunk, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
        chunk.close();
    }

    @Test
    public void testVeryLargeIdThrows() {
        chunk = makeTestChunk(new Object[] {"val"});
        final long tooLarge = ((long) Integer.MAX_VALUE) + 1;
        assertThatThrownBy(() -> registry.update(tooLarge, chunk, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too large");
        chunk.close();
    }

    @Test
    public void testGetWithoutUpdate() {
        assertThat(registry.get(0L)).isNull();
        assertThat(registry.get(1000L)).isNull();
    }

    @Test
    public void testMultipleDictsBothSequentialAndSparse() {
        // Sequential: 0, 1, 2
        for (int id = 0; id < 3; id++) {
            chunk = makeTestChunk(new Object[] {"seq_" + id});
            registry.update(id, chunk, false);
            chunk.close();
        }

        // Sparse: 50
        chunk = makeTestChunk(new Object[] {"sparse_50"});
        registry.update(50L, chunk, false);
        chunk.close();

        assertThat(registry.get(0L)).containsExactly("seq_0");
        assertThat(registry.get(1L)).containsExactly("seq_1");
        assertThat(registry.get(2L)).containsExactly("seq_2");
        assertThat(registry.get(50L)).containsExactly("sparse_50");
        assertThat(registry.get(25L)).isNull();
    }

    @Test
    public void testNonDeltaReplacesCurrent() {
        chunk = makeTestChunk(new Object[] {"original"});
        registry.update(3L, chunk, false);
        chunk.close();

        assertThat(registry.get(3L)).containsExactly("original");

        // Replace with non-delta
        chunk = makeTestChunk(new Object[] {"replaced"});
        registry.update(3L, chunk, false);
        chunk.close();

        assertThat(registry.get(3L)).containsExactly("replaced");
    }

    @Test
    public void testInitialDeltaCreatesNew() {
        // Delta on empty ID should create new list
        chunk = makeTestChunk(new Object[] {"first_val"});
        registry.update(7L, chunk, true);
        chunk.close();

        assertThat(registry.get(7L)).containsExactly("first_val");
    }

    // --- Helper

    private static WritableObjectChunk<Object, Values> makeTestChunk(Object[] values) {
        final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(values.length);
        for (int i = 0; i < values.length; i++) {
            chunk.set(i, values[i]);
        }
        chunk.setSize(values.length);
        return chunk;
    }
}

