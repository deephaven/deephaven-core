package io.deephaven.client.impl;

import io.deephaven.uri.DeephavenTarget;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannelBuilder;
import io.grpc.TlsChannelCredentials;

public final class ChannelHelper {

    public static final int DEFAULT_TLS_PORT = 443;

    public static final int DEFAULT_PLAINTEXT_PORT = 10000;

    /**
     * Creates a basic {@link ManagedChannelBuilder} by invoking
     * {@link Grpc#newChannelBuilder(String, ChannelCredentials)} with {@link DeephavenTarget#toString()} and the
     * appropriate {@link io.grpc.CallCredentials}.
     *
     * @param target the Deephaven target
     * @return the channel builder
     */
    public static ManagedChannelBuilder<?> channelBuilder(DeephavenTarget target) {
        final ChannelCredentials credentials;
        if (target.isSecure()) {
            credentials = TlsChannelCredentials.newBuilder().build();
        } else {
            credentials = InsecureChannelCredentials.create();
        }
        return Grpc.newChannelBuilder(target.toString(), credentials);
    }
}
