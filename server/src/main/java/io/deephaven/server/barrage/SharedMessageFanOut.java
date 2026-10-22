//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.DrainableByteArrayInputStream;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistryImpl;
import io.deephaven.extensions.barrage.chunk.SharedDictionaryWriterState;
import io.deephaven.extensions.barrage.chunk.SharedWriterDictionary;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.grpc.stub.StreamObserver;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

/**
 * Writes one message for a group of full subscribers that would each be sent the same bytes, and delivers it to all of
 * them. Each stream the message is written as is captured into an array, handed to every subscriber still receiving,
 * and dropped before the next stream is written, so the group holds one stream's bytes at a time rather than the whole
 * message.
 *
 * <p>
 * Every subscriber shares the producer's {@link SharedWriterDictionary}s, so their RecordBatches already refer to the
 * same indices; what differs between subscribers is only how many of each dictionary's values each has been sent.
 * {@link #synchronizeDictionaries} brings the subscribers level before the message is written. For each dictionary id
 * it finds the highest offset any subscriber has reached, {@code H}, and gives every subscriber below {@code H} one
 * catch-up of the values from its own offset up to {@code H}: a delta for a subscriber that has been sent a batch, a
 * full replacement from zero for one that needs a full batch. The group's registry then starts at {@code H}, so the
 * message carries only what is new beyond it, which is the same for every subscriber. Once the message has been written
 * to a subscriber, {@link #advanceSubscriberDictionaries} moves the subscriber's states to the group's.
 *
 * <p>
 * The group never starts below {@code H}: a subscriber that already holds the values between a lower offset and
 * {@code H} would append them a second time, and every later index would refer to the wrong value.
 *
 * <p>
 * Every value below {@code H} is in the shared dictionary, since no offset exceeds the dictionary's size within a
 * generation. The producer resets its dictionaries only before a propagation phase's writes begin, and values are only
 * ever appended, so the values below {@code H} stay as they are while other subscribers add to the same dictionaries in
 * parallel.
 *
 * <p>
 * A subscriber whose delivery fails is reported to the {@link DeliveryFailureHandler} once and is sent nothing more;
 * the other subscribers carry on. A catch-up that cannot be built fails only the subscribers owed it. A failure writing
 * the message itself would meet every subscriber alike, so it fails all of them.
 *
 * <p>
 * Thread-safety: a fan-out, and the subscriber states it reads and moves, is used by one thread.
 */
final class SharedMessageFanOut {

    /** Told of each subscriber that is failed, once. */
    interface DeliveryFailureHandler {
        void onFailure(int subIndex, @NotNull Exception failure);
    }

    /** Thrown through the writer to stop writing a message that no subscriber is still receiving. */
    private static final class NoSubscriberReceiving extends RuntimeException {
        private NoSubscriberReceiving() {
            super("no subscriber is still receiving", null, false, false);
        }
    }

    private final List<StreamObserver<BarrageMessageWriter.MessageView>> listeners;
    /** The subscribers' registries, each backed by {@link #sharedDictionaries}. */
    private final List<DictionaryWriterRegistryImpl> registries;
    private final Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries;
    private final DeliveryFailureHandler failureHandler;
    /** Every subscriber, for the messages that go to all of them. */
    private final IntList allSubs;
    private final boolean[] failed;

    /**
     * @param listeners the subscribers' listeners; a subscriber is identified by its index in this list
     * @param registries the subscribers' persistent dictionary registries, in the same order, each backed by
     *        {@code sharedDictionaries}
     * @param sharedDictionaries the producer's shared dictionaries
     * @param failureHandler told of each subscriber that is failed
     * @throws IllegalArgumentException if a subscriber's registry is not backed by the shared dictionaries, or the
     *         subscribers have more or fewer registries than listeners
     */
    SharedMessageFanOut(
            @NotNull final List<StreamObserver<BarrageMessageWriter.MessageView>> listeners,
            @NotNull final List<? extends DictionaryWriterRegistry> registries,
            @NotNull final Long2ObjectOpenHashMap<SharedWriterDictionary> sharedDictionaries,
            @NotNull final DeliveryFailureHandler failureHandler) {
        final int numSubs = listeners.size();
        if (registries.size() != numSubs) {
            throw new IllegalArgumentException(
                    "Expected a registry for each of " + numSubs + " subscribers, got " + registries.size());
        }
        this.listeners = listeners;
        this.registries = new ArrayList<>(numSubs);
        for (final DictionaryWriterRegistry registry : registries) {
            if (!(registry instanceof DictionaryWriterRegistryImpl)
                    || ((DictionaryWriterRegistryImpl) registry).sharedDictionaries() != sharedDictionaries) {
                throw new IllegalArgumentException(
                        "Every subscriber's registry must be backed by the shared dictionaries");
            }
            this.registries.add((DictionaryWriterRegistryImpl) registry);
        }
        this.sharedDictionaries = sharedDictionaries;
        this.failureHandler = failureHandler;
        this.allSubs = new IntArrayList(numSubs);
        for (int si = 0; si < numSubs; ++si) {
            allSubs.add(si);
        }
        this.failed = new boolean[numSubs];
    }

    /**
     * Writes {@code bmw}'s message once and delivers it to every subscriber: first each subscriber's catch-ups, then
     * the message, written with the group's registry. Each subscriber that was sent all of it is then advanced to the
     * group's dictionary position. Failures fail subscribers as the class documentation describes; none is thrown.
     *
     * @param bmw the writer of the propagation phase's message
     * @param options the subscribers' effective subscription options
     * @param reverseViewport the subscribers' viewport direction
     * @param columns the subscribers' subscribed columns
     */
    void propagateToSubscribers(
            @NotNull final BarrageMessageWriter bmw,
            @NotNull final BarrageSubscriptionOptions options,
            final boolean reverseViewport,
            @NotNull final BitSet columns) {
        final DictionaryWriterRegistry groupRegistry;
        try {
            groupRegistry = synchronizeDictionaries(bmw, options);
            deliverMessageToSubscribers(
                    bmw.getSubView(options, false, true, null, reverseViewport, null, null, columns, groupRegistry),
                    allSubs);
        } catch (final Exception e) {
            failAllSubscribers(e);
            return;
        }

        // every subscriber still receiving was sent the whole message
        for (int si = 0; si < failed.length; ++si) {
            if (!failed[si]) {
                try {
                    advanceSubscriberDictionaries(si, groupRegistry);
                } catch (final Exception e) {
                    failSubscriber(si, e);
                }
            }
        }
    }

    /**
     * Brings the subscribers' dictionaries level, as the class documentation describes: sends each subscriber below the
     * highest offset any subscriber has reached its catch-up, and returns the group's registry, which starts at that
     * offset. Subscribers owed the same values share one catch-up, built once, and catch-ups are sent in dictionary id
     * order. Call this after the producer has reset any dictionaries for the propagation phase, and before the message
     * is written.
     *
     * @param bmw writes the catch-ups
     * @param options the subscribers' effective subscription options
     * @return the registry to write the group's message with
     */
    @NotNull
    DictionaryWriterRegistry synchronizeDictionaries(
            @NotNull final BarrageMessageWriter bmw,
            @NotNull final BarrageSubscriptionOptions options) {
        final int numSubs = registries.size();
        final DictionaryWriterRegistryImpl registry = new DictionaryWriterRegistryImpl(sharedDictionaries);

        // Every subscriber has a state for every dictionary id: its snapshot registered each dictionary column, whether
        // or
        // not it subscribes to the column, and the producer's ids are fixed by its schema. So the first subscriber's
        // ids
        // are every subscriber's. They are taken in id order, so that catch-ups are sent in an order that does not
        // depend
        // on hashing.
        final List<DictionaryWriterRegistry.Entry> dictionaryEntries = new ArrayList<>(registries.get(0).entries());
        dictionaryEntries.sort(Comparator.comparingLong(entry -> entry.state.getDictId()));

        final int[] offsets = new int[numSubs];
        final boolean[] needsFull = new boolean[numSubs];
        for (final DictionaryWriterRegistry.Entry dictionaryEntry : dictionaryEntries) {
            final long id = dictionaryEntry.state.getDictId();
            final SharedWriterDictionary shared = ((SharedDictionaryWriterState) dictionaryEntry.state).shared();
            final ChunkWriter<Chunk<Values>> valuesWriter = dictionaryEntry.valuesWriter;
            int high = 0;
            int low = Integer.MAX_VALUE;
            boolean anyNeedsFull = false;
            for (int si = 0; si < numSubs; ++si) {
                final SharedDictionaryWriterState state = subscriberDictionaryState(si, id);
                // needsFullBatch() first detects any reset, which also returns the offset to zero
                needsFull[si] = state.needsFullBatch();
                offsets[si] = state.flushedOffset();
                high = Math.max(high, offsets[si]);
                low = Math.min(low, offsets[si]);
                anyNeedsFull |= needsFull[si];
            }

            // The group starts at H. With H = 0 no subscriber holds a value worth keeping, so the group's first batch
            // replaces every subscriber's dictionary when any of them needs a full batch.
            final SharedDictionaryWriterState groupState = (SharedDictionaryWriterState) registry.getOrCreate(
                    id, valuesWriter, shared.getValuesChunkType());
            groupState.advanceTo(high, high == 0 && anyNeedsFull);

            if (low == high) {
                // every subscriber is level, so none is owed a catch-up
                continue;
            }
            // sort the subscribers owed a catch-up by what they hold: nothing, or the values below their offset
            final IntArrayList fullSubIndices = new IntArrayList();
            final Int2ObjectLinkedOpenHashMap<IntArrayList> deltaSubIndicesByOffset =
                    new Int2ObjectLinkedOpenHashMap<>();
            for (int si = 0; si < numSubs; ++si) {
                if (needsFull[si]) {
                    fullSubIndices.add(si);
                } else if (offsets[si] < high) {
                    IntArrayList deltaSubIndices = deltaSubIndicesByOffset.get(offsets[si]);
                    if (deltaSubIndices == null) {
                        deltaSubIndices = new IntArrayList();
                        deltaSubIndicesByOffset.put(offsets[si], deltaSubIndices);
                    }
                    deltaSubIndices.add(si);
                }
            }
            // subscribers holding no values are sent [0, H) as a replacement, one catch-up for all of them
            deliverDictionaryToSubscribers(bmw, options, shared, valuesWriter, 0, high, false, fullSubIndices);
            // subscribers holding [0, k) are sent [k, H) as a delta, one catch-up for all the subscribers at the same
            // offset k
            for (final Int2ObjectMap.Entry<IntArrayList> entry : deltaSubIndicesByOffset.int2ObjectEntrySet()) {
                deliverDictionaryToSubscribers(bmw, options, shared, valuesWriter, entry.getIntKey(), high, true,
                        entry.getValue());
            }
        }
        return registry;
    }

    /**
     * Sends {@code subIndices} a catch-up of {@code shared}'s values in {@code [fromOffset, toOffset)}: one
     * DictionaryBatch, or several when one would exceed the maximum message size. The catch-up is built only if one of
     * them is still receiving, and a failure building or writing it fails only {@code subIndices}, since only they were
     * owed it.
     */
    private void deliverDictionaryToSubscribers(
            @NotNull final BarrageMessageWriter bmw,
            @NotNull final BarrageSubscriptionOptions options,
            @NotNull final SharedWriterDictionary shared,
            @NotNull final ChunkWriter<Chunk<Values>> valuesWriter,
            final int fromOffset,
            final int toOffset,
            final boolean isDelta,
            @NotNull final IntList subIndices) {
        if (!anyReceiving(subIndices)) {
            return;
        }
        try {
            deliverMessageToSubscribers(bmw.getDictionaryBatchView(options, shared.getDictId(), valuesWriter,
                    shared.buildDeltaChunk(fromOffset, toOffset), isDelta), subIndices);
        } catch (final Exception e) {
            for (int ii = 0; ii < subIndices.size(); ++ii) {
                failSubscriber(subIndices.getInt(ii), e);
            }
        }
    }

    /**
     * Writes {@code view} once, handing each of its streams to every one of {@code subIndices} still receiving before
     * the next stream is written. Stops writing as soon as none of them is receiving. A subscriber whose delivery fails
     * is failed alone.
     *
     * @param view the view of the message
     * @param subIndices the indices of the subscribers to send it to
     * @throws IOException if writing the message fails; a stream that cannot be captured fails the write too, and is
     *         thrown, while delivery failures are not
     */
    void deliverMessageToSubscribers(@NotNull final BarrageMessageWriter.MessageView view,
            @NotNull final IntList subIndices)
            throws IOException {
        try {
            // Each stream of the message covers one record batch. This could be a dictionary batch or a regular data
            // batch. The writer builds each stream only once this visitor has returned from the one before, so every
            // subscriber is handed batch k before batch k + 1 is encoded.
            view.forEachStream(stream -> {
                // If every subscriber has failed, stop the writer, since no one is left to encode the rest for. The
                // visitor owns each stream it is handed, so it closes this one first.
                if (!anyReceiving(subIndices)) {
                    closeQuietly(stream);
                    throw new NoSubscriberReceiving();
                }

                // The stream is captured once, then delivered to each subscriber. Capturing drains it into an array
                // of exactly the size it reports, and closing it releases the column data it refers to.
                final byte[] bytes;
                try (stream) {
                    final int size = stream.available();
                    final ExposedByteArrayOutputStream out = new ExposedByteArrayOutputStream(size);
                    stream.drainTo(out);
                    if (out.size() != size) {
                        throw new IllegalStateException(
                                "A stream wrote " + out.size() + " bytes, not the " + size + " it reported available");
                    }
                    // sized to the stream and never grown, so the buffer is exactly the bytes written
                    bytes = out.peekBuffer();
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }

                // TODO: distribute the captured batch in parallel if the number of subscribers or message size is
                // large. This is a pure copy operation but does add time per subscriber.
                for (int ii = 0; ii < subIndices.size(); ++ii) {
                    final int subIndex = subIndices.getInt(ii);
                    // a subscriber that has failed is sent nothing more
                    if (failed[subIndex]) {
                        continue;
                    }
                    try {
                        // each subscriber is given its own stream over the array, since draining a stream takes its
                        // bytes; gRPC copies them into its transport buffers before onNext returns
                        listeners.get(subIndex).onNext(
                                visitor -> visitor.accept(new DrainableByteArrayInputStream(bytes, 0, bytes.length)));
                    } catch (final Exception e) {
                        // a subscriber whose listener throws is failed alone, and the others carry on
                        failSubscriber(subIndex, e);
                    }
                }
            });
        } catch (final NoSubscriberReceiving ignored) {
            // no subscriber is left to write the rest of the message for
        }
    }

    /**
     * Moves subscriber {@code subIndex}'s states to those of {@code groupRegistry}, once the subscriber has been sent
     * its catch-ups and all of the group's message. Do not call this for a subscriber that missed part of the message:
     * it would then be credited with values it was never sent.
     */
    void advanceSubscriberDictionaries(final int subIndex, @NotNull final DictionaryWriterRegistry groupRegistry) {
        for (final DictionaryWriterRegistry.Entry groupEntry : groupRegistry.entries()) {
            final SharedDictionaryWriterState groupState = (SharedDictionaryWriterState) groupEntry.state;
            subscriberDictionaryState(subIndex, groupState.getDictId())
                    .advanceTo(groupState.flushedOffset(), groupState.needsFullBatch());
        }
    }

    /** Subscriber {@code subIndex}'s state for dictionary {@code dictId}, which every subscriber has. */
    @NotNull
    private SharedDictionaryWriterState subscriberDictionaryState(final int subIndex, final long dictId) {
        final DictionaryWriterRegistry.Entry entry = registries.get(subIndex).getEntry(dictId);
        if (entry == null) {
            throw new IllegalStateException("Subscriber " + subIndex + " has no state for dictionary id " + dictId);
        }
        return (SharedDictionaryWriterState) entry.state;
    }

    /** Whether any of {@code subIndices} has not been failed. */
    private boolean anyReceiving(@NotNull final IntList subIndices) {
        for (int ii = 0; ii < subIndices.size(); ++ii) {
            if (!failed[subIndices.getInt(ii)]) {
                return true;
            }
        }
        return false;
    }

    /** Fails subscriber {@code subIndex}, if it is still receiving, and tells the failure handler. */
    void failSubscriber(final int subIndex, @NotNull final Exception failure) {
        if (failed[subIndex]) {
            return;
        }
        failed[subIndex] = true;
        failureHandler.onFailure(subIndex, failure);
    }

    /** Fails every subscriber still receiving. */
    void failAllSubscribers(@NotNull final Exception failure) {
        for (int si = 0; si < failed.length; ++si) {
            failSubscriber(si, failure);
        }
    }

    private static void closeQuietly(@NotNull final DefensiveDrainable stream) {
        try {
            stream.close();
        } catch (final IOException ignored) {
            // nothing was going to be done with it
        }
    }
}
