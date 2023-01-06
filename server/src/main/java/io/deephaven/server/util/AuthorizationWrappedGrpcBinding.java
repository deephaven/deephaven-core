package io.deephaven.server.util;

import io.deephaven.auth.ServiceAuthWiring;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;

public class AuthorizationWrappedGrpcBinding<ServiceImplBase> implements BindableService {
    private final ServiceAuthWiring<ServiceImplBase> authWiring;
    private final ServiceImplBase delegate;

    public AuthorizationWrappedGrpcBinding(ServiceAuthWiring<ServiceImplBase> authWiring, ServiceImplBase delegate) {
        this.authWiring = authWiring;
        this.delegate = delegate;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return authWiring.intercept(delegate);
    }
}
