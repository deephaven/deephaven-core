package io.deephaven.grpc_api.auth;

import com.google.protobuf.ByteString;
import io.deephaven.util.auth.AuthContext;

/**
 * The AuthContextProvider's job is to manage all authentication and authorization responsibilities.
 * If audit logging is required, an implementation of this (and its companion AuthContext) is the
 * correct way path forward.
 */
public interface AuthContextProvider {
    /**
     * Returns true if this auth context provider can authenticate using the provided protocol
     * version.
     * 
     * @param authProtocol the protocol version to use (application specific)
     * @return true iff `authProtocol` is supported
     */
    boolean supportsProtocol(long authProtocol);

    /**
     * Authenticate and obtain an {@link AuthContext} using the provided protocol and byte payload.
     * 
     * @param protocolVersion the protocol version to use (application specific)
     * @param payload the user's login details (protocol specific)
     * @return null if user could not be authenticated, otherwise an AuthContext for the user
     */
    AuthContext authenticate(long protocolVersion, ByteString payload);
}
