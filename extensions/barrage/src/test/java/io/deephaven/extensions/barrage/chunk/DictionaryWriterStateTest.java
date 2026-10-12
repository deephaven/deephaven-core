//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryWriterStateTest {

    private static final BarrageSubscriptionOptions OPTS = BarrageSubscriptionOptions.builder().build();

    /** Calls fillIndexChunk with a single Object value and returns the resulting index. */
    private static int indexForObject(final DictionaryWriterState state, final Object value) {
        try (final WritableObjectChunk<Object, Values> src = WritableObjectChunk.makeWritableChunk(1);
                final WritableIntChunk<Values> out = WritableIntChunk.makeWritableChunk(1)) {
            src.set(0, value);
            src.setSize(1);
            out.setSize(1);
            state.fillIndexChunk(src, null, OPTS, out);
            return out.get(0);
        }
    }

    /** Calls fillIndexChunk on the shared dictionary with a single Object value and returns the resulting index. */
    private static int indexForObject(final SharedWriterDictionary shared, final Object value) {
        try (final WritableObjectChunk<Object, Values> src = WritableObjectChunk.makeWritableChunk(1);
                final WritableIntChunk<Values> out = WritableIntChunk.makeWritableChunk(1)) {
            src.set(0, value);
            src.setSize(1);
            out.setSize(1);
            shared.fillIndexChunk(src, null, OPTS, out);
            return out.get(0);
        }
    }

    /** Drains buildDeltaChunk() into a List for assertion. Closes the returned chunk. */
    private static List<Object> deltaObjectValues(final DictionaryWriterState state) {
        try (final WritableChunk<Values> chunk = state.buildDeltaChunk()) {
            final ObjectChunk<Object, Values> oc = chunk.asObjectChunk();
            final List<Object> result = new ArrayList<>(oc.size());
            for (int i = 0; i < oc.size(); i++) {
                result.add(oc.get(i));
            }
            return result;
        }
    }

    // ---- LocalDictionaryWriterState (viewport / snapshot) ---------------

    @Test
    public void testLocalFirstBatchUsesInsertionOrderAndStableIndexes() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(17, ChunkType.Object);

        assertThat(state.hasDelta()).isTrue();
        assertThat(state.needsFullBatch()).isTrue();
        assertThat(indexForObject(state, "alpha")).isZero();
        assertThat(indexForObject(state, "beta")).isEqualTo(1);
        assertThat(indexForObject(state, "alpha")).isZero();

        assertThat(deltaObjectValues(state)).containsExactly("alpha", "beta");
    }

    @Test
    public void testLocalResetDeltaExposesOnlyNewValues() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(23, ChunkType.Object);

        indexForObject(state, "alpha");
        indexForObject(state, "beta");
        state.resetDelta();

        assertThat(state.hasDelta()).isFalse();
        assertThat(state.needsFullBatch()).isFalse();
        assertThat(indexForObject(state, "beta")).isEqualTo(1);
        assertThat(state.hasDelta()).isFalse();

        assertThat(indexForObject(state, "gamma")).isEqualTo(2);
        assertThat(indexForObject(state, "delta")).isEqualTo(3);
        assertThat(deltaObjectValues(state)).containsExactly("gamma", "delta");

        state.resetDelta();
        assertThat(state.hasDelta()).isFalse();
    }

    // ---- SharedDictionaryWriterState (full / growing subscriptions) -

    @Test
    public void testFullSubscriptionFirstBatchContainsAllCurrentValues() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(5L, ChunkType.Object);
        // Pre-populate the shared dictionary (simulates values added by earlier subscribers)
        indexForObject(shared, "alpha");
        indexForObject(shared, "beta");

        // New subscriber joins after those values were already added
        final DictionaryWriterState sub = new SharedDictionaryWriterState(shared);

        assertThat(sub.hasDelta()).isTrue();
        assertThat(sub.needsFullBatch()).isTrue();
        // First batch must contain ALL current values so isDelta=false reset covers them
        assertThat(deltaObjectValues(sub)).containsExactly("alpha", "beta");
    }

    @Test
    public void testFullSubscriptionResetDeltaRetainsFullList() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(6L, ChunkType.Object);

        final DictionaryWriterState sub = new SharedDictionaryWriterState(shared);
        indexForObject(sub, "alpha");
        indexForObject(sub, "beta");
        assertThat(deltaObjectValues(sub)).containsExactly("alpha", "beta");
        sub.resetDelta();

        assertThat(sub.hasDelta()).isFalse();
        assertThat(sub.needsFullBatch()).isFalse();

        // New value added after reset
        indexForObject(sub, "gamma");
        assertThat(sub.hasDelta()).isTrue();
        assertThat(deltaObjectValues(sub)).containsExactly("gamma");

        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();
    }

    @Test
    public void testFullSubscriptionIndexesAreSharedAcrossSubscribers() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(7L, ChunkType.Object);

        final DictionaryWriterState sub1 = new SharedDictionaryWriterState(shared);
        final DictionaryWriterState sub2 = new SharedDictionaryWriterState(shared);

        // sub1 adds values
        assertThat(indexForObject(sub1, "foo")).isEqualTo(0);
        assertThat(indexForObject(sub1, "bar")).isEqualTo(1);

        // sub2 sees the same indexes because they share the underlying state
        assertThat(indexForObject(sub2, "foo")).isEqualTo(0);
        assertThat(indexForObject(sub2, "bar")).isEqualTo(1);
        assertThat(indexForObject(sub2, "baz")).isEqualTo(2);

        // sub1 also sees baz (added by sub2) at the same index
        assertThat(indexForObject(sub1, "baz")).isEqualTo(2);
    }

    @Test
    public void testNewSubscriberReceivesAllValuesBeforeJoining() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(8L, ChunkType.Object);

        // First subscriber pumps some values
        final DictionaryWriterState sub1 = new SharedDictionaryWriterState(shared);
        indexForObject(sub1, "a");
        indexForObject(sub1, "b");
        assertThat(deltaObjectValues(sub1)).containsExactly("a", "b");
        sub1.resetDelta();
        indexForObject(sub1, "c");
        assertThat(deltaObjectValues(sub1)).containsExactly("c");
        sub1.resetDelta();

        // Second subscriber joins after sub1 has already sent a, b, c
        final DictionaryWriterState sub2 = new SharedDictionaryWriterState(shared);
        assertThat(sub2.needsFullBatch()).isTrue();
        // Must see all three values as the initial reset batch
        assertThat(deltaObjectValues(sub2)).containsExactly("a", "b", "c");
        sub2.resetDelta();

        // Now a new value is added; both sub1 and sub2 only see the delta
        indexForObject(sub1, "d");
        assertThat(deltaObjectValues(sub1)).containsExactly("d");
        assertThat(deltaObjectValues(sub2)).containsExactly("d");
    }

    // ---- LocalDictionaryWriterState.reset() (compaction) --------------------

    @Test
    public void testLocalResetClearsAllStateAndForcesFullBatch() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(42, ChunkType.Object);

        indexForObject(state, "x");
        indexForObject(state, "y");
        state.resetDelta();
        assertThat(state.hasDelta()).isFalse();
        assertThat(state.totalSize()).isEqualTo(2);

        state.reset();

        assertThat(state.hasDelta()).isTrue();
        assertThat(state.needsFullBatch()).isTrue();
        assertThat(state.totalSize()).isEqualTo(0);
        assertThat(deltaObjectValues(state)).isEmpty();

        // Indexes start over from 0 after reset
        assertThat(indexForObject(state, "x")).isZero();
        assertThat(deltaObjectValues(state)).containsExactly("x");
    }

    @Test
    public void testLocalTotalSizeAndDictId() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(99, ChunkType.Object);

        assertThat(state.getDictId()).isEqualTo(99L);
        assertThat(state.totalSize()).isZero();

        indexForObject(state, "a");
        indexForObject(state, "b");
        assertThat(state.totalSize()).isEqualTo(2);

        indexForObject(state, "a"); // duplicate, no change
        assertThat(state.totalSize()).isEqualTo(2);

        state.resetDelta();
        assertThat(state.totalSize()).isEqualTo(2);
    }

    // ---- SharedDictionaryWriterState generation-based reset -------------

    @Test
    public void testFullSubscriptionDetectsSharedReset() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(9L, ChunkType.Object);

        final DictionaryWriterState sub = new SharedDictionaryWriterState(shared);
        indexForObject(sub, "a");
        indexForObject(sub, "b");
        assertThat(deltaObjectValues(sub)).containsExactly("a", "b");
        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();
        assertThat(sub.needsFullBatch()).isFalse();

        // Simulate compaction: shared dictionary is reset externally
        shared.reset();

        // Subscriber must detect the generation bump on next call
        assertThat(sub.needsFullBatch()).isTrue();
        assertThat(sub.hasDelta()).isTrue();

        // buildDeltaChunk returns the (now-empty) shared list — subscriber will re-send whatever gets re-added
        assertThat(deltaObjectValues(sub)).isEmpty();

        // After re-adding values, subscriber sees them as a fresh full batch
        indexForObject(shared, "c");
        assertThat(deltaObjectValues(sub)).containsExactly("c");
    }

    @Test
    public void testFullSubscriptionResetDeltaAfterSharedResetSyncsCorrectly() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(10L, ChunkType.Object);
        final DictionaryWriterState sub = new SharedDictionaryWriterState(shared);

        indexForObject(sub, "a");
        assertThat(deltaObjectValues(sub)).containsExactly("a");
        sub.resetDelta();

        shared.reset();
        indexForObject(shared, "b");

        // The first call into the sub after a shared reset triggers syncGeneration, and the batch built after it is a
        // full replacement holding only the value re-added since the reset
        assertThat(sub.needsFullBatch()).isTrue();
        assertThat(deltaObjectValues(sub)).containsExactly("b");
        sub.resetDelta();

        // After resetDelta post-reset, flushedOffset advances to the new shared size (1)
        // so no further delta is pending
        assertThat(sub.hasDelta()).isFalse();
        assertThat(sub.needsFullBatch()).isFalse();
    }

    @Test
    public void testFullSubscriptionMultipleResetsTrackGeneration() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(11L, ChunkType.Object);
        final DictionaryWriterState sub = new SharedDictionaryWriterState(shared);

        indexForObject(sub, "a");
        assertThat(deltaObjectValues(sub)).containsExactly("a");
        sub.resetDelta();

        // First compaction
        shared.reset();
        assertThat(sub.needsFullBatch()).isTrue();
        indexForObject(sub, "b");
        assertThat(deltaObjectValues(sub)).containsExactly("b");
        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();

        // Second compaction
        shared.reset();
        assertThat(sub.needsFullBatch()).isTrue();
        assertThat(deltaObjectValues(sub)).isEmpty();
        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();
    }

    @Test
    public void testFullSubscriptionResetThrows() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(12L, ChunkType.Object);
        final DictionaryWriterState sub = new SharedDictionaryWriterState(shared);

        try {
            sub.reset();
            throw new AssertionError("Expected UnsupportedOperationException");
        } catch (final UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testFullSubscriptionTotalSizeAndDictId() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(13L, ChunkType.Object);
        final DictionaryWriterState sub = new SharedDictionaryWriterState(shared);

        assertThat(sub.getDictId()).isEqualTo(13L);
        assertThat(sub.totalSize()).isZero();

        indexForObject(sub, "x");
        indexForObject(sub, "y");
        assertThat(sub.totalSize()).isEqualTo(2);

        sub.resetDelta();
        assertThat(sub.totalSize()).isEqualTo(2);
    }

    @Test
    public void testSubscriberCreatedAfterSharedResetDoesNotSeeStaleGeneration() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(14L, ChunkType.Object);

        // Simulate a round of usage + compaction before any subscriber attaches
        final DictionaryWriterState earlySubscriber = new SharedDictionaryWriterState(shared);
        indexForObject(earlySubscriber, "old");
        earlySubscriber.resetDelta();
        shared.reset();

        // New subscriber joins after the reset; it should not treat this as a post-reset event
        final DictionaryWriterState lateSubscriber = new SharedDictionaryWriterState(shared);
        indexForObject(shared, "new");

        // needsFullBatch is true on first use regardless (normal first-batch behavior)
        assertThat(lateSubscriber.needsFullBatch()).isTrue();
        // The late subscriber's "full" batch contains only the values present after the reset
        assertThat(deltaObjectValues(lateSubscriber)).containsExactly("new");
    }

    // ---- SharedDictionaryWriterState with full subscribers writing in parallel -------------

    /**
     * Full subscribers write in parallel, so another subscriber can add a value between this one building its
     * DictionaryBatch and that batch being accepted. Accepting it must advance past only the values the batch carried;
     * the late value is still owed, and the next delta must carry it.
     */
    @Test
    public void testFullSubscriptionResetDeltaAdvancesOnlyPastBuiltValues() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(15L, ChunkType.Object);
        final DictionaryWriterState sub1 = new SharedDictionaryWriterState(shared);
        final DictionaryWriterState sub2 = new SharedDictionaryWriterState(shared);

        indexForObject(sub1, "a");
        indexForObject(sub1, "b");
        // sub1 builds the DictionaryBatch that precedes its RecordBatch...
        assertThat(deltaObjectValues(sub1)).containsExactly("a", "b");
        // ...and sub2, writing at the same time, adds a value that batch does not carry
        assertThat(indexForObject(sub2, "c")).isEqualTo(2);

        // sub1's batch is accepted
        sub1.resetDelta();

        assertThat(sub1.hasDelta()).isTrue();
        assertThat(sub1.needsFullBatch()).isFalse();
        assertThat(deltaObjectValues(sub1)).containsExactly("c");
    }

    /**
     * A batch that needed no DictionaryBatch still ends with the registry resetting every state that has a delta by
     * then. If another subscriber added a value in the meantime, this state has a delta it never sent, and the reset
     * must leave it owed.
     */
    @Test
    public void testFullSubscriptionResetDeltaWithoutBuiltDeltaKeepsLateValues() {
        final SharedWriterDictionary shared = new SharedWriterDictionary(16L, ChunkType.Object);
        final DictionaryWriterState sub1 = new SharedDictionaryWriterState(shared);
        final DictionaryWriterState sub2 = new SharedDictionaryWriterState(shared);

        indexForObject(sub1, "a");
        assertThat(deltaObjectValues(sub1)).containsExactly("a");
        sub1.resetDelta();

        // sub1's next batch references only "a", so the writer finds no delta and builds no DictionaryBatch
        assertThat(indexForObject(sub1, "a")).isZero();
        assertThat(sub1.hasDelta()).isFalse();
        // sub2 adds a value before sub1's batch is accepted
        indexForObject(sub2, "b");
        assertThat(sub1.hasDelta()).isTrue();

        // accepting sub1's batch resets each state that has a delta now, sub1's included
        sub1.resetDelta();

        assertThat(sub1.hasDelta()).isTrue();
        assertThat(deltaObjectValues(sub1)).containsExactly("b");
    }

    /**
     * Full subscribers on one shared dictionary write their batches from different threads at once, each doing what the
     * writer does for a batch: fill the index chunk, emit a DictionaryBatch if there is a delta, then accept the batch.
     * Every index each subscriber's client receives must resolve, through the values that client was sent, to the value
     * it encodes.
     */
    @Test
    public void testConcurrentFullSubscribersReceiveEveryValueTheyReference() throws Exception {
        final SharedWriterDictionary shared = new SharedWriterDictionary(17L, ChunkType.Object);
        final int numSubscribers = 8;
        final int numBatches = 400;
        final int batchSize = 64;
        final int numDistinctValues = 20_000;

        final ExecutorService executor = Executors.newFixedThreadPool(numSubscribers);
        try {
            final CyclicBarrier start = new CyclicBarrier(numSubscribers);
            final List<Future<Integer>> subscribers = new ArrayList<>();
            for (int si = 0; si < numSubscribers; ++si) {
                final long seed = si;
                subscribers.add(executor.submit(() -> {
                    final DictionaryWriterState state = new SharedDictionaryWriterState(shared);
                    // the dictionary as this subscriber's client has received it
                    final List<Object> client = new ArrayList<>();
                    final Random random = new Random(seed);
                    start.await();
                    for (int bi = 0; bi < numBatches; ++bi) {
                        try (final WritableObjectChunk<Object, Values> src =
                                WritableObjectChunk.makeWritableChunk(batchSize);
                                final WritableIntChunk<Values> out = WritableIntChunk.makeWritableChunk(batchSize)) {
                            for (int ri = 0; ri < batchSize; ++ri) {
                                src.set(ri, "v" + random.nextInt(numDistinctValues));
                            }
                            src.setSize(batchSize);
                            out.setSize(batchSize);
                            state.fillIndexChunk(src, null, OPTS, out);

                            if (state.hasDelta()) {
                                if (state.needsFullBatch()) {
                                    client.clear();
                                }
                                client.addAll(deltaObjectValues(state));
                            }
                            for (int ri = 0; ri < batchSize; ++ri) {
                                final int index = out.get(ri);
                                assertThat(index).isLessThan(client.size());
                                assertThat(client.get(index)).isEqualTo(src.get(ri));
                            }
                            state.resetDelta();
                        }
                    }
                    return client.size();
                }));
            }
            for (final Future<Integer> subscriber : subscribers) {
                assertThat(subscriber.get(2, TimeUnit.MINUTES)).isLessThanOrEqualTo(shared.getTotalSize());
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
