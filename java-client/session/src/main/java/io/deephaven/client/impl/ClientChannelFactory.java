//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.grpc.ManagedChannel;

public interface ClientChannelFactory {

    /**
     * The default client channel factory. Equivalent to {@link ChannelHelper#channel(ClientConfig)}.
     *
     * @return the default client channel factory.
     */
    static ClientChannelFactory defaultInstance() {
        return ChannelHelper::channel;
    }

    ManagedChannel create(ClientConfig clientConfig);
}
