//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto;

import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.stub.AbstractStub;

import java.util.Objects;

final class DeephavenChannelWithClientInterceptors extends DeephavenChannelMixin {
    private final ClientInterceptor[] clientInterceptors;

    public DeephavenChannelWithClientInterceptors(DeephavenChannel delegate, ClientInterceptor... clientInterceptors) {
        super(delegate);
        this.clientInterceptors = Objects.requireNonNull(clientInterceptors);
    }

    @Override
    protected <S extends AbstractStub<S>> S mixin(S stub) {
        return stub.withInterceptors(clientInterceptors);
    }

    @Override
    protected Channel mixinChannel(Channel channel) {
        return ClientInterceptors.intercept(channel, clientInterceptors);
    }
}
