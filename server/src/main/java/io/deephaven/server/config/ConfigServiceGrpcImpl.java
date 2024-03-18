//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.config;

import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigValue;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.deephaven.server.session.SessionService;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Serves specified configuration properties to gRPC clients.
 */
public class ConfigServiceGrpcImpl extends ConfigServiceGrpc.ConfigServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ConfigServiceGrpcImpl.class);
    private static final String VERSION_LIST_PROPERTY = "client.version.list";

    private static final String AUTH_CLIENT_CONFIG_PROPERTY = "authentication.client.configuration.list";

    // TODO consider a mechanism for roles for these
    private static final String CLIENT_CONFIG_PROPERTY = "client.configuration.list";

    private final Configuration configuration = Configuration.getInstance();
    private final SessionService sessionService;

    @Inject
    public ConfigServiceGrpcImpl(SessionService sessionService) {
        this.sessionService = sessionService;
        // On startup, lookup the versions to make available.
        for (String pair : configuration.getStringArrayFromProperty(VERSION_LIST_PROPERTY)) {
            pair = pair.trim();
            if (pair.isEmpty()) {
                continue;
            }
            String[] split = pair.split("=");
            if (split.length != 2) {
                throw new IllegalArgumentException("Missing '=' in " + VERSION_LIST_PROPERTY);
            }
            String key = split[0] + ".version";
            if (configuration.hasProperty(key)) {
                throw new IllegalArgumentException("Configuration already has a key for '" + key + "'");
            }
            String className = split[1];
            try {
                configuration.setProperty(key, Class.forName(className, false, getClass().getClassLoader()).getPackage()
                        .getImplementationVersion());
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Failed to find class to get its version '" + className + "'");
            }
        }
    }

    @Override
    public void getAuthenticationConstants(
            @NotNull final AuthenticationConstantsRequest request,
            @NotNull final StreamObserver<AuthenticationConstantsResponse> responseObserver) {
        AuthenticationConstantsResponse.Builder builder = AuthenticationConstantsResponse.newBuilder();
        builder.putAllConfigValues(collectConfigs(AUTH_CLIENT_CONFIG_PROPERTY));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getConfigurationConstants(
            @NotNull final ConfigurationConstantsRequest request,
            @NotNull final StreamObserver<ConfigurationConstantsResponse> responseObserver) {
        // Read the current session so we throw if not authenticated
        sessionService.getCurrentSession();

        ConfigurationConstantsResponse.Builder builder = ConfigurationConstantsResponse.newBuilder();
        builder.putAllConfigValues(collectConfigs(CLIENT_CONFIG_PROPERTY));
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private Map<String, ConfigValue> collectConfigs(String clientConfigProperty) {
        return Arrays.stream(configuration.getStringWithDefault(clientConfigProperty, "").split(","))
                .map(String::trim)
                .filter(key -> !key.isEmpty())
                .map(key -> {
                    String value = configuration.getStringWithDefault(key, null);
                    if (value == null) {
                        return null;
                    }
                    return Map.entry(key, ConfigValue.newBuilder().setStringValue(value).build());
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
