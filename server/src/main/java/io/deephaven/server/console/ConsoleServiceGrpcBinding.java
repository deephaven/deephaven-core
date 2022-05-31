package io.deephaven.server.console;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse;
import io.deephaven.proto.backplane.script.grpc.BrowserNextResponse;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
import io.deephaven.server.browserstreaming.BrowserStream;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.util.GrpcServiceOverrideBuilder;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;

import javax.inject.Inject;

public class ConsoleServiceGrpcBinding implements BindableService {
    private static final Logger log = LoggerFactory.getLogger(ConsoleServiceGrpcBinding.class);
    private final ConsoleServiceGrpcImpl delegate;
    private final SessionService sessionService;

    @Inject
    public ConsoleServiceGrpcBinding(ConsoleServiceGrpcImpl delegate, SessionService sessionService) {
        this.delegate = delegate;
        this.sessionService = sessionService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService())
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
