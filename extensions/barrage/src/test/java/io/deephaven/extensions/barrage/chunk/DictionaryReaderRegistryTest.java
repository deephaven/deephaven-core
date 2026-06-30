//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryReaderRegistryTest {

    @Test
    public void testGetUnknownIdReturnsNull() {
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
        assertThat(registry.get(0L)).isNull();
        assertThat(registry.get(99L)).isNull();
    }

    @Test
    public void testUpdateReplaceInstallsNewDictionary() {
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();

        try (final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(3)) {
            chunk.set(0, "cat");
            chunk.set(1, "dog");
            chunk.set(2, "fish");
            chunk.setSize(3);
            registry.update(0L, chunk, false);
        }

        final List<Object> dict = registry.get(0L);
        assertThat(dict).containsExactly("cat", "dog", "fish");
    }

    @Test
    public void testUpdateDeltaAppendsToExisting() {
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();

        try (final WritableObjectChunk<Object, Values> chunk1 = WritableObjectChunk.makeWritableChunk(2)) {
            chunk1.set(0, "alpha");
            chunk1.set(1, "beta");
            chunk1.setSize(2);
            registry.update(0L, chunk1, false);
        }

        try (final WritableObjectChunk<Object, Values> chunk2 = WritableObjectChunk.makeWritableChunk(2)) {
            chunk2.set(0, "gamma");
            chunk2.set(1, "delta");
            chunk2.setSize(2);
            registry.update(0L, chunk2, true);
        }

        final List<Object> dict = registry.get(0L);
        assertThat(dict).containsExactly("alpha", "beta", "gamma", "delta");
    }

    @Test
    public void testUpdateReplaceOverwritesPreviousDictionary() {
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();

        try (final WritableObjectChunk<Object, Values> chunk1 = WritableObjectChunk.makeWritableChunk(2)) {
            chunk1.set(0, "old1");
            chunk1.set(1, "old2");
            chunk1.setSize(2);
            registry.update(5L, chunk1, false);
        }

        // Replace (isDelta=false) should discard the old list.
        try (final WritableObjectChunk<Object, Values> chunk2 = WritableObjectChunk.makeWritableChunk(1)) {
            chunk2.set(0, "new1");
            chunk2.setSize(1);
            registry.update(5L, chunk2, false);
        }

        final List<Object> dict = registry.get(5L);
        assertThat(dict).containsExactly("new1");
    }

    @Test
    public void testMultipleIdsAreIndependent() {
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();

        try (final WritableIntChunk<Values> chunkA = WritableIntChunk.makeWritableChunk(2)) {
            chunkA.set(0, 10);
            chunkA.set(1, 20);
            chunkA.setSize(2);
            registry.update(0L, chunkA, false);
        }

        try (final WritableIntChunk<Values> chunkB = WritableIntChunk.makeWritableChunk(2)) {
            chunkB.set(0, 30);
            chunkB.set(1, 40);
            chunkB.setSize(2);
            registry.update(1L, chunkB, false);
        }

        assertThat(registry.get(0L)).containsExactly(10, 20);
        assertThat(registry.get(1L)).containsExactly(30, 40);
    }

    @Test
    public void testDeltaOnNewIdActsLikeInitialLoad() {
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();

        // Sending isDelta=true for a brand-new id should still populate the dict.
        try (final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(2)) {
            chunk.set(0, "x");
            chunk.set(1, "y");
            chunk.setSize(2);
            registry.update(7L, chunk, true);
        }

        assertThat(registry.get(7L)).containsExactly("x", "y");
    }

    @Test
    public void testNullValuesRoundTripAsNull() {
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();

        try (final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(3)) {
            chunk.set(0, "a");
            chunk.set(1, null);
            chunk.set(2, "b");
            chunk.setSize(3);
            registry.update(0L, chunk, false);
        }

        final List<Object> dict = registry.get(0L);
        assertThat(dict).hasSize(3);
        assertThat(dict.get(0)).isEqualTo("a");
        assertThat(dict.get(1)).isNull();
        assertThat(dict.get(2)).isEqualTo("b");
    }
}
