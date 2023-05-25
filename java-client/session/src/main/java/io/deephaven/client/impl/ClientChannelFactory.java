package io.deephaven.client.impl;

import io.grpc.ManagedChannel;

public interface ClientChannelFactory {
    static ClientChannelFactory defaultInstance() {
        return ChannelHelper::channel;
    }

    ManagedChannel create(ClientConfig clientConfig);
}
