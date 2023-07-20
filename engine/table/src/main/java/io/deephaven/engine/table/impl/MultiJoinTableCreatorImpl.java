/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import com.google.auto.service.AutoService;
import io.deephaven.engine.table.MultiJoinFactory;
import io.deephaven.engine.table.MultiJoinInput;
import io.deephaven.engine.table.MultiJoinTable;
import org.jetbrains.annotations.NotNull;

/**
 * Engine-specific implementation of {@link MultiJoinFactory.Creator}.
 */
@SuppressWarnings("unused")
public enum MultiJoinTableCreatorImpl implements MultiJoinFactory.Creator {
    INSTANCE;

    @Override
    public MultiJoinTable of(@NotNull MultiJoinInput... multiJoinInputs) {
        if (multiJoinInputs.length == 0) {
            throw new IllegalArgumentException("At least one table must be included in MultiJoinTable.");
        }
        return MultiJoinTableImpl.of(multiJoinInputs);
    }

    @SuppressWarnings("unused")
    @AutoService(MultiJoinFactory.CreatorProvider.class)
    public static final class ProviderImpl implements MultiJoinFactory.CreatorProvider {
        @Override
        public MultiJoinFactory.Creator get() {
            return INSTANCE;
        }
    }
}
