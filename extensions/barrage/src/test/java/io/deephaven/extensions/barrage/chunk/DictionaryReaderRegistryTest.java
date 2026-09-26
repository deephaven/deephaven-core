//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryReaderRegistryTest {

    // Note: update() copies values into the registry's internal storage; the caller retains ownership of the chunks
    // it passes in and remains responsible for closing them.

    @Test
    public void testGetUnknownIdReturnsNull() {
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry()) {
            assertThat(registry.get(0L)).isNull();
            assertThat(registry.get(99L)).isNull();
        }
    }

    @Test
    public void testUpdateReplaceInstallsNewDictionary() {
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(3)) {
            chunk.set(0, "cat");
            chunk.set(1, "dog");
            chunk.set(2, "fish");
            chunk.setSize(3);
            registry.update(0L, chunk, false);

            final DictionaryReaderValues dict = registry.get(0L);
            assertThat(dict).isNotNull();
            assertThat(dict.size()).isEqualTo(3);
            assertThat(dict.<String>getObject(0)).isEqualTo("cat");
            assertThat(dict.<String>getObject(1)).isEqualTo("dog");
            assertThat(dict.<String>getObject(2)).isEqualTo("fish");
        }
    }

    @Test
    public void testUpdateDeltaAppendsToExisting() {
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableObjectChunk<Object, Values> chunk1 = WritableObjectChunk.makeWritableChunk(2);
                final WritableObjectChunk<Object, Values> chunk2 = WritableObjectChunk.makeWritableChunk(2)) {
            chunk1.set(0, "alpha");
            chunk1.set(1, "beta");
            chunk1.setSize(2);
            registry.update(0L, chunk1, false);

            chunk2.set(0, "gamma");
            chunk2.set(1, "delta");
            chunk2.setSize(2);
            registry.update(0L, chunk2, true);

            final DictionaryReaderValues dict = registry.get(0L);
            assertThat(dict).isNotNull();
            assertThat(dict.size()).isEqualTo(4);
            assertThat(dict.<String>getObject(0)).isEqualTo("alpha");
            assertThat(dict.<String>getObject(1)).isEqualTo("beta");
            assertThat(dict.<String>getObject(2)).isEqualTo("gamma");
            assertThat(dict.<String>getObject(3)).isEqualTo("delta");
        }
    }

    @Test
    public void testUpdateReplaceOverwritesPreviousDictionary() {
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableObjectChunk<Object, Values> chunk1 = WritableObjectChunk.makeWritableChunk(2);
                final WritableObjectChunk<Object, Values> chunk2 = WritableObjectChunk.makeWritableChunk(1)) {
            chunk1.set(0, "old1");
            chunk1.set(1, "old2");
            chunk1.setSize(2);
            registry.update(5L, chunk1, false);

            // Replace (isDelta=false) should discard the old values.
            chunk2.set(0, "new1");
            chunk2.setSize(1);
            registry.update(5L, chunk2, false);

            final DictionaryReaderValues dict = registry.get(5L);
            assertThat(dict).isNotNull();
            assertThat(dict.size()).isEqualTo(1);
            assertThat(dict.<String>getObject(0)).isEqualTo("new1");
        }
    }

    @Test
    public void testMultipleIdsAreIndependent() {
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableIntChunk<Values> chunkA = WritableIntChunk.makeWritableChunk(2);
                final WritableIntChunk<Values> chunkB = WritableIntChunk.makeWritableChunk(2)) {
            chunkA.set(0, 10);
            chunkA.set(1, 20);
            chunkA.setSize(2);
            registry.update(0L, chunkA, false);

            chunkB.set(0, 30);
            chunkB.set(1, 40);
            chunkB.setSize(2);
            registry.update(1L, chunkB, false);

            final DictionaryReaderValues dictA = registry.get(0L);
            assertThat(dictA).isNotNull();
            assertThat(dictA.size()).isEqualTo(2);
            assertThat(dictA.getInt(0)).isEqualTo(10);
            assertThat(dictA.getInt(1)).isEqualTo(20);

            final DictionaryReaderValues dictB = registry.get(1L);
            assertThat(dictB).isNotNull();
            assertThat(dictB.size()).isEqualTo(2);
            assertThat(dictB.getInt(0)).isEqualTo(30);
            assertThat(dictB.getInt(1)).isEqualTo(40);
        }
    }

    @Test
    public void testDeltaOnNewIdActsLikeInitialLoad() {
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(2)) {
            // Sending isDelta=true for a brand-new id should still populate the dict.
            chunk.set(0, "x");
            chunk.set(1, "y");
            chunk.setSize(2);
            registry.update(7L, chunk, true);

            final DictionaryReaderValues dict = registry.get(7L);
            assertThat(dict).isNotNull();
            assertThat(dict.size()).isEqualTo(2);
            assertThat(dict.<String>getObject(0)).isEqualTo("x");
            assertThat(dict.<String>getObject(1)).isEqualTo("y");
        }
    }

    @Test
    public void testNullValuesRoundTripAsNull() {
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableObjectChunk<Object, Values> chunk = WritableObjectChunk.makeWritableChunk(3)) {
            chunk.set(0, "a");
            chunk.set(1, null);
            chunk.set(2, "b");
            chunk.setSize(3);
            registry.update(0L, chunk, false);

            final DictionaryReaderValues dict = registry.get(0L);
            assertThat(dict).isNotNull();
            assertThat(dict.size()).isEqualTo(3);
            assertThat(dict.<String>getObject(0)).isEqualTo("a");
            assertThat(dict.<String>getObject(1)).isNull();
            assertThat(dict.<String>getObject(2)).isEqualTo("b");
        }
    }

    @Test
    public void testAppendAcrossChunkBoundary() {
        final int chunkCapacity = DictionaryReaderValues.CHUNK_CAPACITY;
        final int totalSize = chunkCapacity + chunkCapacity / 2;
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableIntChunk<Values> chunk1 = WritableIntChunk.makeWritableChunk(chunkCapacity - 7);
                final WritableIntChunk<Values> chunk2 =
                        WritableIntChunk.makeWritableChunk(totalSize - (chunkCapacity - 7))) {
            // The first chunk stops just short of the chunk boundary; the second spans it.
            for (int ii = 0; ii < chunk1.size(); ++ii) {
                chunk1.set(ii, ii);
            }
            registry.update(0L, chunk1, false);
            for (int ii = 0; ii < chunk2.size(); ++ii) {
                chunk2.set(ii, chunk1.size() + ii);
            }
            registry.update(0L, chunk2, true);

            final DictionaryReaderValues dict = registry.get(0L);
            assertThat(dict).isNotNull();
            assertThat(dict.size()).isEqualTo(totalSize);
            assertThat(dict.getInt(0)).isEqualTo(0);
            assertThat(dict.getInt(chunkCapacity - 1)).isEqualTo(chunkCapacity - 1);
            assertThat(dict.getInt(chunkCapacity)).isEqualTo(chunkCapacity);
            assertThat(dict.getInt(totalSize - 1)).isEqualTo(totalSize - 1);
        }
    }

    @Test
    public void testReplaceAfterMultiChunkDictionary() {
        final int chunkCapacity = DictionaryReaderValues.CHUNK_CAPACITY;
        try (final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableIntChunk<Values> bigChunk = WritableIntChunk.makeWritableChunk(chunkCapacity + 100);
                final WritableIntChunk<Values> smallChunk = WritableIntChunk.makeWritableChunk(2)) {
            for (int ii = 0; ii < bigChunk.size(); ++ii) {
                bigChunk.set(ii, ii);
            }
            registry.update(0L, bigChunk, false);
            assertThat(registry.get(0L).size()).isEqualTo(chunkCapacity + 100);

            // Replace should release the internal chunks and expose only the new values.
            smallChunk.set(0, -1);
            smallChunk.set(1, -2);
            smallChunk.setSize(2);
            registry.update(0L, smallChunk, false);

            final DictionaryReaderValues dict = registry.get(0L);
            assertThat(dict).isNotNull();
            assertThat(dict.size()).isEqualTo(2);
            assertThat(dict.getInt(0)).isEqualTo(-1);
            assertThat(dict.getInt(1)).isEqualTo(-2);
        }
    }
}
