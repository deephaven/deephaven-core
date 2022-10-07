package io.deephaven.server.config;

import io.deephaven.configuration.Configuration;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse;
import io.deephaven.proto.backplane.grpc.ConfigPair;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

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

    @Inject
    public ConfigServiceGrpcImpl() {
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
    public void getAuthenticationConstants(AuthenticationConstantsRequest request,
            StreamObserver<AuthenticationConstantsResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            AuthenticationConstantsResponse.Builder builder = AuthenticationConstantsResponse.newBuilder();
            collectConfigs(builder::addConfigValues, AUTH_CLIENT_CONFIG_PROPERTY);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getConfigurationConstants(ConfigurationConstantsRequest request,
            StreamObserver<ConfigurationConstantsResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            ConfigurationConstantsResponse.Builder builder = ConfigurationConstantsResponse.newBuilder();
            collectConfigs(builder::addConfigValues, CLIENT_CONFIG_PROPERTY);
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    private void collectConfigs(Consumer<ConfigPair> addConfigValues, String clientConfigProperty) {
        Arrays.stream(configuration.getStringWithDefault(clientConfigProperty, "").split(","))
                .map(String::trim)
                .filter(key -> !key.isEmpty())
                .map(key -> {
                    String value = configuration.getStringWithDefault(key, null);
                    if (value == null) {
                        return null;
                    }
                    return ConfigPair.newBuilder()
                            .setKey(key)
                            .setStringValue(value)
                            .build();
                })
                .filter(Objects::nonNull)
                .forEach(addConfigValues);
    }
}
