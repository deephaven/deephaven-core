package io.deephaven.client.impl;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Represents the authentication information required for a {@link Session}.
 */
@Immutable
@SimpleStyle
public abstract class AuthenticationInfo {

    public static AuthenticationInfo of(HandshakeResponse response) {
        return ImmutableAuthenticationInfo.of(response.getMetadataHeader().toStringUtf8(),
                response.getSessionToken().toStringUtf8());
    }

    @Parameter
    public abstract String sessionHeaderKey();

    @Parameter
    public abstract String session();
}
