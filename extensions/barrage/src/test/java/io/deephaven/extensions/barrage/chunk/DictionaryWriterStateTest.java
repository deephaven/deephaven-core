//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DictionaryWriterStateTest {

    // ---- LocalDictionaryWriterState (viewport / snapshot) ---------------

    @Test
    public void testLocalFirstBatchUsesInsertionOrderAndStableIndexes() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(17);

        assertThat(state.hasDelta()).isTrue();
        assertThat(state.needsFullBatch()).isTrue();
        assertThat(state.indexFor("alpha")).isZero();
        assertThat(state.indexFor("beta")).isEqualTo(1);
        assertThat(state.indexFor("alpha")).isZero();

        assertThat(state.getDeltaValues()).containsExactly("alpha", "beta");
    }

    @Test
    public void testLocalResetDeltaExposesOnlyNewValues() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(23);

        state.indexFor("alpha");
        state.indexFor("beta");
        state.resetDelta();

        assertThat(state.hasDelta()).isFalse();
        assertThat(state.needsFullBatch()).isFalse();
        assertThat(state.indexFor("beta")).isEqualTo(1);
        assertThat(state.hasDelta()).isFalse();

        assertThat(state.indexFor("gamma")).isEqualTo(2);
        assertThat(state.indexFor("delta")).isEqualTo(3);
        assertThat(state.getDeltaValues()).containsExactly("gamma", "delta");

        state.resetDelta();
        assertThat(state.hasDelta()).isFalse();
    }

    // ---- FullSubscriptionDictionaryState (full / growing subscriptions) -

    @Test
    public void testFullSubscriptionFirstBatchContainsAllCurrentValues() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(5L);
        // Pre-populate the shared state (simulates values added by earlier subscribers)
        shared.indexFor("alpha");
        shared.indexFor("beta");

        // New subscriber joins after those values were already added
        final DictionaryWriterState sub = new FullSubscriptionDictionaryState(shared);

        assertThat(sub.hasDelta()).isTrue();
        assertThat(sub.needsFullBatch()).isTrue();
        // First batch must contain ALL current values so isDelta=false reset covers them
        assertThat(sub.getDeltaValues()).containsExactly("alpha", "beta");
    }

    @Test
    public void testFullSubscriptionResetDeltaRetainsFullList() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(6L);

        final DictionaryWriterState sub = new FullSubscriptionDictionaryState(shared);
        sub.indexFor("alpha");
        sub.indexFor("beta");
        sub.resetDelta();

        assertThat(sub.hasDelta()).isFalse();
        assertThat(sub.needsFullBatch()).isFalse();

        // New value added after reset
        sub.indexFor("gamma");
        assertThat(sub.hasDelta()).isTrue();
        assertThat(sub.getDeltaValues()).containsExactly("gamma");

        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();
    }

    @Test
    public void testFullSubscriptionIndexesAreSharedAcrossSubscribers() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(7L);

        final DictionaryWriterState sub1 = new FullSubscriptionDictionaryState(shared);
        final DictionaryWriterState sub2 = new FullSubscriptionDictionaryState(shared);

        // sub1 adds values
        assertThat(sub1.indexFor("foo")).isEqualTo(0);
        assertThat(sub1.indexFor("bar")).isEqualTo(1);

        // sub2 sees the same indexes because they share the underlying state
        assertThat(sub2.indexFor("foo")).isEqualTo(0);
        assertThat(sub2.indexFor("bar")).isEqualTo(1);
        assertThat(sub2.indexFor("baz")).isEqualTo(2);

        // sub1 also sees baz (added by sub2) at the same index
        assertThat(sub1.indexFor("baz")).isEqualTo(2);
    }

    @Test
    public void testNewSubscriberReceivesAllValuesBeforeJoining() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(8L);

        // First subscriber pumps some values
        final DictionaryWriterState sub1 = new FullSubscriptionDictionaryState(shared);
        sub1.indexFor("a");
        sub1.indexFor("b");
        sub1.resetDelta();
        sub1.indexFor("c");
        sub1.resetDelta();

        // Second subscriber joins after sub1 has already sent a, b, c
        final DictionaryWriterState sub2 = new FullSubscriptionDictionaryState(shared);
        assertThat(sub2.needsFullBatch()).isTrue();
        // Must see all three values as the initial reset batch
        assertThat(sub2.getDeltaValues()).containsExactly("a", "b", "c");
        sub2.resetDelta();

        // Now a new value is added; both sub1 and sub2 only see the delta
        sub1.indexFor("d");
        assertThat(sub1.getDeltaValues()).containsExactly("d");
        assertThat(sub2.getDeltaValues()).containsExactly("d");
    }

    // ---- LocalDictionaryWriterState.reset() (compaction) --------------------

    @Test
    public void testLocalResetClearsAllStateAndForcesFullBatch() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(42);

        state.indexFor("x");
        state.indexFor("y");
        state.resetDelta();
        assertThat(state.hasDelta()).isFalse();
        assertThat(state.totalSize()).isEqualTo(2);

        state.reset();

        assertThat(state.hasDelta()).isTrue();
        assertThat(state.needsFullBatch()).isTrue();
        assertThat(state.totalSize()).isEqualTo(0);
        assertThat(state.getDeltaValues()).isEmpty();

        // Indexes start over from 0 after reset
        assertThat(state.indexFor("x")).isZero();
        assertThat(state.getDeltaValues()).containsExactly("x");
    }

    @Test
    public void testLocalTotalSizeAndDictId() {
        final DictionaryWriterState state = new LocalDictionaryWriterState(99);

        assertThat(state.getDictId()).isEqualTo(99L);
        assertThat(state.totalSize()).isZero();

        state.indexFor("a");
        state.indexFor("b");
        assertThat(state.totalSize()).isEqualTo(2);

        state.indexFor("a"); // duplicate, no change
        assertThat(state.totalSize()).isEqualTo(2);

        state.resetDelta();
        assertThat(state.totalSize()).isEqualTo(2);
    }

    // ---- FullSubscriptionDictionaryState generation-based reset -------------

    @Test
    public void testFullSubscriptionDetectsSharedReset() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(9L);

        final DictionaryWriterState sub = new FullSubscriptionDictionaryState(shared);
        sub.indexFor("a");
        sub.indexFor("b");
        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();
        assertThat(sub.needsFullBatch()).isFalse();

        // Simulate compaction: shared dictionary is reset externally
        shared.reset();

        // Subscriber must detect the generation bump on next call
        assertThat(sub.needsFullBatch()).isTrue();
        assertThat(sub.hasDelta()).isTrue();

        // getDeltaValues returns the (now-empty) shared list — subscriber will re-send whatever gets re-added
        assertThat(sub.getDeltaValues()).isEmpty();

        // After re-adding values, subscriber sees them as a fresh full batch
        shared.indexFor("c");
        assertThat(sub.getDeltaValues()).containsExactly("c");
    }

    @Test
    public void testFullSubscriptionResetDeltaAfterSharedResetSyncsCorrectly() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(10L);
        final DictionaryWriterState sub = new FullSubscriptionDictionaryState(shared);

        sub.indexFor("a");
        sub.resetDelta();

        shared.reset();
        shared.indexFor("b");

        // First call into the sub after a shared reset triggers syncGeneration
        sub.resetDelta();

        // After resetDelta post-reset, flushedOffset advances to the new shared size (1)
        // so no further delta is pending
        assertThat(sub.hasDelta()).isFalse();
        assertThat(sub.needsFullBatch()).isFalse();
    }

    @Test
    public void testFullSubscriptionMultipleResetsTrackGeneration() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(11L);
        final DictionaryWriterState sub = new FullSubscriptionDictionaryState(shared);

        sub.indexFor("a");
        sub.resetDelta();

        // First compaction
        shared.reset();
        assertThat(sub.needsFullBatch()).isTrue();
        sub.indexFor("b");
        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();

        // Second compaction
        shared.reset();
        assertThat(sub.needsFullBatch()).isTrue();
        sub.resetDelta();
        assertThat(sub.hasDelta()).isFalse();
    }

    @Test
    public void testFullSubscriptionResetThrows() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(12L);
        final DictionaryWriterState sub = new FullSubscriptionDictionaryState(shared);

        try {
            sub.reset();
            throw new AssertionError("Expected UnsupportedOperationException");
        } catch (final UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testFullSubscriptionTotalSizeAndDictId() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(13L);
        final DictionaryWriterState sub = new FullSubscriptionDictionaryState(shared);

        assertThat(sub.getDictId()).isEqualTo(13L);
        assertThat(sub.totalSize()).isZero();

        sub.indexFor("x");
        sub.indexFor("y");
        assertThat(sub.totalSize()).isEqualTo(2);

        sub.resetDelta();
        assertThat(sub.totalSize()).isEqualTo(2);
    }

    @Test
    public void testSubscriberCreatedAfterSharedResetDoesNotSeeStaleGeneration() {
        final SharedDictionaryWriterState shared = new SharedDictionaryWriterState(14L);

        // Simulate a round of usage + compaction before any subscriber attaches
        final DictionaryWriterState earlySubscriber = new FullSubscriptionDictionaryState(shared);
        earlySubscriber.indexFor("old");
        earlySubscriber.resetDelta();
        shared.reset();

        // New subscriber joins after the reset; it should not treat this as a post-reset event
        final DictionaryWriterState lateSubscriber = new FullSubscriptionDictionaryState(shared);
        shared.indexFor("new");

        // needsFullBatch is true on first use regardless (normal first-batch behavior)
        assertThat(lateSubscriber.needsFullBatch()).isTrue();
        // The late subscriber's "full" batch contains only the values present after the reset
        assertThat(lateSubscriber.getDeltaValues()).containsExactly("new");
    }
}
