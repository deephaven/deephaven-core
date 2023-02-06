/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.testutil.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ImmutableColumnHolder<T> extends ColumnHolder<T> {
    public ImmutableColumnHolder(@NotNull final String name, @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType, final boolean grouped, final Chunk<Values> data) {
        super(false, name, dataType, componentType, grouped, data);
    }
}
