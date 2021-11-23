/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ImmutableColumnHolder<T> extends ColumnHolder<T> {
    @SuppressWarnings("unchecked")
    public ImmutableColumnHolder(@NotNull final String name, @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType, final boolean grouped, final T... data) {
        super(name, dataType, componentType, grouped, data);
    }
}
