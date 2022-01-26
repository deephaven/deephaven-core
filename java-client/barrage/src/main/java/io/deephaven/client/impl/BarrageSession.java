/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.qst.table.TableSpec;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
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

    private final Channel interceptedChannel;

    protected BarrageSession(
            final SessionImpl session, final FlightClient client, final ManagedChannel channel) {
        super(session, client);
        this.interceptedChannel = ClientInterceptors.intercept(channel, new AuthInterceptor());
    }

    @Override
    public BarrageSubscription subscribe(final TableSpec tableSpec, final BarrageSubscriptionOptions options)
            throws TableHandle.TableHandleException, InterruptedException {
        try (final TableHandle handle = session().execute(tableSpec)) {
            return subscribe(handle, options);
        }
    }

    @Override
    public BarrageSubscription subscribe(final TableHandle tableHandle, final BarrageSubscriptionOptions options) {
        return new BarrageSubscriptionImpl(this, tableHandle.newRef(), options);
    }

    @Override
    public BarrageSnapshot snapshot(final TableSpec tableSpec, final BarrageSnapshotOptions options)
            throws TableHandle.TableHandleException, InterruptedException {
        try (final TableHandle handle = session().execute(tableSpec)) {
            return snapshot(handle, options);
        }
    }

    @Override
    public BarrageSnapshot snapshot(final TableHandle tableHandle, final BarrageSnapshotOptions options) {
        return new BarrageSnapshotImpl(this, tableHandle.newRef(), options);
    }

    public Channel channel() {
        return interceptedChannel;
    }

    private class AuthInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                final Channel channel) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    channel.newCall(methodDescriptor, callOptions)) {
                @Override
                public void start(final Listener<RespT> responseListener, final Metadata headers) {
                    final AuthenticationInfo localAuth = ((SessionImpl) session()).auth();
                    headers.put(Metadata.Key.of(localAuth.sessionHeaderKey(), Metadata.ASCII_STRING_MARSHALLER),
                            localAuth.session());
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
