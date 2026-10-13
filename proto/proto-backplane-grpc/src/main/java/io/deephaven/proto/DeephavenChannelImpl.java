//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto;

import io.grpc.Channel;

import javax.inject.Inject;

/**
 * A Deephaven service helper for a {@link Channel channel}.
 *
 * @deprecated use {@link DeephavenChannel}
 */
@Deprecated
public final class DeephavenChannelImpl extends DeephavenChannel {

    @Inject
    public DeephavenChannelImpl(Channel channel) {
        super(channel);
    }
}
