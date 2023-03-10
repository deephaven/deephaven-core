package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.ConfigValue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ConfigService {
    CompletableFuture<Map<String, ConfigValue>> getAuthenticationConstants();
    CompletableFuture<Map<String, ConfigValue>> getConfigurationConstants();
}
