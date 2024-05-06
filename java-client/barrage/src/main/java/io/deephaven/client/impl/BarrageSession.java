//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.util.annotations.InternalUseOnly;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Collections;

public class BarrageSession extends FlightSession implements BarrageSubscription.Factory, BarrageSnapshot.Factory {

    /**
     * Creates a barrage session. Closing the barrage session does <b>not</b> close {@code channel}.
     *
     * @param session the session
     * @param incomingAllocator the incoming allocator
     * @param channel the managed channel
     * @return the barrage session
     */
    public static BarrageSession of(
            SessionImpl session, BufferAllocator incomingAllocator, ManagedChannel channel) {
        final FlightClient client = FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(
                incomingAllocator, channel, Collections.singletonList(new SessionMiddleware(session)));
        return new BarrageSession(session, client);
    }

    /**
     * @apiNote This method exists to be called by the Python API. It will be removed in the future if we can make JPY
     *          capable of selecting the right factory method to use when the same method is present in the class
     *          hierarchy multiple times.
     * @see #of(SessionImpl, BufferAllocator, ManagedChannel)
     */
    @InternalUseOnly
    public static BarrageSession create(
            SessionImpl session, BufferAllocator incomingAllocator, ManagedChannel channel) {
        return BarrageSession.of(session, incomingAllocator, channel);
    }

    protected BarrageSession(
            final SessionImpl session, final FlightClient client) {
        super(session, client);
    }

    @Override
    public BarrageSubscription subscribe(final TableSpec tableSpec, final BarrageSubscriptionOptions options)
            throws TableHandle.TableHandleException, InterruptedException {
        try (final TableHandle handle = session.execute(tableSpec)) {
            return subscribe(handle, options);
        }
    }

    @Override
    public BarrageSubscription subscribe(final TableHandle tableHandle, final BarrageSubscriptionOptions options) {
        return BarrageSubscription.make(this, session.executor(), tableHandle.newRef(), options);
    }

    @Override
    public BarrageSnapshot snapshot(final TableSpec tableSpec, final BarrageSnapshotOptions options)
            throws TableHandle.TableHandleException, InterruptedException {
        try (final TableHandle handle = session.execute(tableSpec)) {
            return snapshot(handle, options);
        }
    }

    @Override
    public BarrageSnapshot snapshot(final TableHandle tableHandle, final BarrageSnapshotOptions options) {
        return new BarrageSnapshotImpl(this, session.executor(), tableHandle.newRef(), options);
    }

    /**
     * The authenticated channel.
     *
     * @return the authenticated channel
     */
    public DeephavenChannel channel() {
        return session.channel();
    }
}
