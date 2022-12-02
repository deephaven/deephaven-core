/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.testutil.sources;

import io.deephaven.engine.table.impl.util.ColumnHolder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ImmutableColumnHolder<T> extends ColumnHolder<T> {
    @SuppressWarnings("unchecked")
    public ImmutableColumnHolder(@NotNull final String name, @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType, final boolean grouped, final T... data) {
        super(name, dataType, componentType, grouped, data);
    }
}
