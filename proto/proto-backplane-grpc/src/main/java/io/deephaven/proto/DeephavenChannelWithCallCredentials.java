//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto;

import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractStub;

import java.util.Objects;

final class DeephavenChannelWithCallCredentials extends DeephavenChannelMixin {
    private final CallCredentials callCredentials;

    public DeephavenChannelWithCallCredentials(DeephavenChannel delegate, CallCredentials callCredentials) {
        super(delegate);
        this.callCredentials = Objects.requireNonNull(callCredentials);
    }

    @Override
    protected <S extends AbstractStub<S>> S mixin(S stub) {
        return stub.withCallCredentials(callCredentials);
    }

    @Override
    protected Channel mixinChannel(Channel channel) {
        return new CallCredentialsChannel(channel);
    }

    private final class CallCredentialsChannel extends Channel {
        private final Channel delegate;

        public CallCredentialsChannel(Channel delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return delegate.newCall(methodDescriptor, callOptions.withCallCredentials(callCredentials));
        }

        @Override
        public String authority() {
            return delegate.authority();
        }
    }
}
