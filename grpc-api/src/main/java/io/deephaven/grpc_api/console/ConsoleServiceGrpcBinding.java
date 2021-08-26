package io.deephaven.grpc_api.console;

import io.deephaven.grpc_api.browserstreaming.BrowserStream;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.util.GrpcServiceOverrideBuilder;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteRequest;
import io.deephaven.proto.backplane.script.grpc.AutoCompleteResponse;
import io.deephaven.proto.backplane.script.grpc.BrowserNextResponse;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
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
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService(), ConsoleServiceGrpc.SERVICE_NAME)
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
