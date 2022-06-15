/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations;

import io.deephaven.util.annotations.FinalDefault;

import javax.annotation.concurrent.Immutable;

/**
 * Sub-interface of {@link TableKey} to mark immutable implementations.
 */
@Immutable
public interface ImmutableTableKey extends TableKey {

    @FinalDefault
    default ImmutableTableKey makeImmutable() {
        return this;
    }
}
