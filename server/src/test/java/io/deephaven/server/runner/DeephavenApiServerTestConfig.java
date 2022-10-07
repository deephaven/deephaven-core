package io.deephaven.server.runner;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.server.config.ServerConfig;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class DeephavenApiServerTestConfig implements ServerConfig {

    public static Builder builder() {
        return ImmutableDeephavenApiServerTestConfig.builder();
    }

    public interface Builder extends ServerConfig.Builder<DeephavenApiServerTestConfig, Builder> {

    }
}
