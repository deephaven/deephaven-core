//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import org.apache.arrow.memory.BufferAllocator;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

@Immutable
@BuildableStyle
public abstract class FlightSessionConfig {

    public static Builder builder() {
        return ImmutableFlightSessionConfig.builder();
    }

    /**
     * The session config.
     */
    public abstract Optional<SessionConfig> sessionConfig();

    /**
     * The allocator.
     */
    public abstract Optional<BufferAllocator> allocator();

    public interface Builder {

        Builder sessionConfig(SessionConfig sessionConfig);

        Builder allocator(BufferAllocator allocator);

        FlightSessionConfig build();
    }
}
