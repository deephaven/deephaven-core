//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.options;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;

import io.deephaven.engine.validation.ColumnExpressionValidator;

/**
 * A set of options for a plugin derived from dagger injection.
 */
@Value.Immutable
@BuildableStyle
public abstract class PluginOptions {
    /**
     * @return the {@link ColumnExpressionValidator} to use for user-provided formulas.
     */
    public abstract ColumnExpressionValidator columnExpressionValidator();

    public static ImmutablePluginOptions.Builder builder() {
        return ImmutablePluginOptions.builder();
    }
}
