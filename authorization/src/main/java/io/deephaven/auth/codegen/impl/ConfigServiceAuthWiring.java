//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth.codegen.impl;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.ServiceAuthWiring;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.grpc.ServerServiceDefinition;

/**
 * This interface provides type-safe authorization hooks for ConfigServiceGrpc.
 */
public interface ConfigServiceAuthWiring extends ServiceAuthWiring<ConfigServiceGrpc.ConfigServiceImplBase> {
    /**
     * Wrap the real implementation with authorization checks.
     *
     * @param delegate the real service implementation
     * @return the wrapped service implementation
     */
    default ServerServiceDefinition intercept(ConfigServiceGrpc.ConfigServiceImplBase delegate) {
        final ServerServiceDefinition service = delegate.bindService();
        final ServerServiceDefinition.Builder serviceBuilder =
                ServerServiceDefinition.builder(service.getServiceDescriptor());

        serviceBuilder.addMethod(ServiceAuthWiring.intercept(
                service, "GetAuthenticationConstants", null, this::onMessageReceivedGetAuthenticationConstants));
        serviceBuilder.addMethod(ServiceAuthWiring.intercept(
                service, "GetConfigurationConstants", null, this::onMessageReceivedGetConfigurationConstants));

        return serviceBuilder.build();
    }

    /**
     * Authorize a request to GetAuthenticationConstants.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke GetAuthenticationConstants
     */
    void onMessageReceivedGetAuthenticationConstants(AuthContext authContext,
            AuthenticationConstantsRequest request);

    /**
     * Authorize a request to GetConfigurationConstants.
     *
     * @param authContext the authentication context of the request
     * @param request the request to authorize
     * @throws io.grpc.StatusRuntimeException if the user is not authorized to invoke GetConfigurationConstants
     */
    void onMessageReceivedGetConfigurationConstants(AuthContext authContext,
            ConfigurationConstantsRequest request);

    class AllowAll implements ConfigServiceAuthWiring {
        public void onMessageReceivedGetAuthenticationConstants(AuthContext authContext,
                AuthenticationConstantsRequest request) {}

        public void onMessageReceivedGetConfigurationConstants(AuthContext authContext,
                ConfigurationConstantsRequest request) {}
    }

    class DenyAll implements ConfigServiceAuthWiring {
        public void onMessageReceivedGetAuthenticationConstants(AuthContext authContext,
                AuthenticationConstantsRequest request) {
            ServiceAuthWiring.operationNotAllowed();
        }

        public void onMessageReceivedGetConfigurationConstants(AuthContext authContext,
                ConfigurationConstantsRequest request) {
            ServiceAuthWiring.operationNotAllowed();
        }
    }

    class TestUseOnly implements ConfigServiceAuthWiring {
        public ConfigServiceAuthWiring delegate;

        public void onMessageReceivedGetAuthenticationConstants(AuthContext authContext,
                AuthenticationConstantsRequest request) {
            if (delegate != null) {
                delegate.onMessageReceivedGetAuthenticationConstants(authContext, request);
            }
        }

        public void onMessageReceivedGetConfigurationConstants(AuthContext authContext,
                ConfigurationConstantsRequest request) {
            if (delegate != null) {
                delegate.onMessageReceivedGetConfigurationConstants(authContext, request);
            }
        }
    }
}
