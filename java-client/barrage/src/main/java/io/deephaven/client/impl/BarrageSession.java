/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.qst.table.TableSpec;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.memory.BufferAllocator;

import java.util.Collections;

public class BarrageSession extends FlightSession implements BarrageSubscription.Factory, BarrageSnapshot.Factory {

    public static BarrageSession of(
            SessionImpl session, BufferAllocator incomingAllocator, ManagedChannel channel) {
        final FlightClient client = FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(
                incomingAllocator, channel, Collections.singletonList(new SessionMiddleware(session)));
        return new BarrageSession(session, client, channel);
    }

    protected BarrageSession(
            final SessionImpl session, final FlightClient client, final ManagedChannel channel) {
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
        return new BarrageSubscriptionImpl(this, session.executor(), tableHandle.newRef(), options);
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
