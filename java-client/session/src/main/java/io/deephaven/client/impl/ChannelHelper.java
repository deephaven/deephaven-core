package io.deephaven.client.impl;

import io.deephaven.uri.DeephavenTarget;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class ChannelHelper {

    public static final int DEFAULT_TLS_PORT = 8080;

    public static final int DEFAULT_PLAINTEXT_PORT = 8080;

    public static ManagedChannel channel(DeephavenTarget target) {
        return channelBuilder(target).build();
    }

    public static ManagedChannelBuilder<?> channelBuilder(DeephavenTarget target) {
        final ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(target.host(), port(target));
        if (target.isSecure()) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        return builder;
    }

    public static int port(DeephavenTarget target) {
        if (target.port().isPresent()) {
            return target.port().getAsInt();
        }
        // TODO(deephaven-core#1489): Support service discovery for DeephavenTarget
        if (target.isSecure()) {
            return Integer.getInteger("deephaven.target.port", DEFAULT_TLS_PORT);
        }
        return Integer.getInteger("deephaven.target.plaintext_port", DEFAULT_PLAINTEXT_PORT);
    }
}
