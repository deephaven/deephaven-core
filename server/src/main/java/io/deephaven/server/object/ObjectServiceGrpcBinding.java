//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.BrowserNextResponse;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.deephaven.proto.backplane.grpc.StreamResponse;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.browserstreaming.BrowserStream;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.GrpcServiceOverrideBuilder;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ObjectServiceGrpcBinding implements BindableService {
    private static final Logger log = LoggerFactory.getLogger(ObjectServiceGrpcBinding.class);
    private final AuthorizationProvider authProvider;
    private final ObjectServiceGrpcImpl delegate;
    private final SessionService sessionService;

    @Inject
    public ObjectServiceGrpcBinding(AuthorizationProvider authProvider, ObjectServiceGrpcImpl delegate,
            SessionService sessionService) {
        this.authProvider = authProvider;
        this.delegate = delegate;
        this.sessionService = sessionService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        ServerServiceDefinition authIntercepted = authProvider.getObjectServiceAuthWiring().intercept(delegate);
        return GrpcServiceOverrideBuilder.newBuilder(authIntercepted)
                .onBidiOverrideWithBrowserSupport(delegate::messageStream,
                        ObjectServiceGrpc.getMessageStreamMethod(),
                        ObjectServiceGrpc.getOpenMessageStreamMethod(),
                        ObjectServiceGrpc.getNextMessageStreamMethod(),
                        ProtoUtils.marshaller(StreamRequest.getDefaultInstance()),
                        ProtoUtils.marshaller(StreamResponse.getDefaultInstance()),
                        ProtoUtils.marshaller(BrowserNextResponse.getDefaultInstance()),
                        BrowserStream.Mode.IN_ORDER,
                        log,
                        sessionService)
                .build();
    }
}
