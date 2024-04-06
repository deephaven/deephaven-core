//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

import java.util.Objects;
import java.util.concurrent.Executor;

import static io.deephaven.client.impl.Authentication.AUTHORIZATION_HEADER;

class AuthenticationCallCredentials extends CallCredentials {
    private final String authenticationTypeAndValue;

    public AuthenticationCallCredentials(String authenticationTypeAndValue) {
        this.authenticationTypeAndValue = Objects.requireNonNull(authenticationTypeAndValue);
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        final Metadata headers = new Metadata();
        headers.put(AUTHORIZATION_HEADER, authenticationTypeAndValue);
        applier.apply(headers);
    }

    @Override
    public void thisUsesUnstableApi() {

    }
}
