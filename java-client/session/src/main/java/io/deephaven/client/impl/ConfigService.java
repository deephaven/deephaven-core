//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.ConfigValue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Provides server-specified configuration values to gRPC clients.
 */
public interface ConfigService {
    /**
     * Returns constants from the server that may be helpful to correctly authenticate with the server. As such,
     * authentication is not required to obtain these values.
     */
    CompletableFuture<Map<String, ConfigValue>> getAuthenticationConstants();

    /**
     * Returns constants from the server that are specified as being appropriate for clients to read. By default these
     * include values like the suggested authentication token refresh interval, and the server-side version of
     * deephaven, barrage, and java.
     */
    CompletableFuture<Map<String, ConfigValue>> getConfigurationConstants();
}
