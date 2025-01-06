//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

import javax.annotation.Nullable;

@Value.Immutable
@BuildableStyle
public interface ExecuteCodeOptions {
    ExecuteCodeOptions DEFAULT = ExecuteCodeOptions.builder().build();

    @Value.Default
    @Nullable
    default Boolean executeSystemic() {
        return null;
    }

    static Builder builder() {
        return ImmutableExecuteCodeOptions.builder();
    }

    interface Builder {
        ExecuteCodeOptions.Builder executeSystemic(Boolean executeSystemic);

        ExecuteCodeOptions build();
    }
}
