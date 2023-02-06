/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.console;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse;
import io.deephaven.proto.backplane.script.grpc.BrowserNextResponse;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.browserstreaming.BrowserStream;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.GrpcServiceOverrideBuilder;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;

import javax.inject.Inject;

public class ConsoleServiceGrpcBinding implements BindableService {
    private static final Logger log = LoggerFactory.getLogger(ConsoleServiceGrpcBinding.class);
    private final AuthorizationProvider authProvider;
    private final ConsoleServiceGrpcImpl delegate;
    private final SessionService sessionService;

    @Inject
    public ConsoleServiceGrpcBinding(
            AuthorizationProvider authProvider,
            ConsoleServiceGrpcImpl delegate,
            SessionService sessionService) {
        this.authProvider = authProvider;
        this.delegate = delegate;
        this.sessionService = sessionService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        ServerServiceDefinition authIntercepted = authProvider.getConsoleServiceAuthWiring().intercept(delegate);
        return GrpcServiceOverrideBuilder.newBuilder(authIntercepted)
                .onBidiOverrideWithBrowserSupport(delegate::autoCompleteStream,
                        ConsoleServiceGrpc.getAutoCompleteStreamMethod(),
                        ConsoleServiceGrpc.getOpenAutoCompleteStreamMethod(),
                        ConsoleServiceGrpc.getNextAutoCompleteStreamMethod(),
                        ProtoUtils.marshaller(AutoCompleteRequest.getDefaultInstance()),
                        ProtoUtils.marshaller(AutoCompleteResponse.getDefaultInstance()),
                        ProtoUtils.marshaller(BrowserNextResponse.getDefaultInstance()),
                        BrowserStream.Mode.IN_ORDER,
                        log,
                        sessionService)
                .build();
    }
}
