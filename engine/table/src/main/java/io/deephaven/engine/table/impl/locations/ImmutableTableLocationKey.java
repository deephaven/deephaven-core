/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations;

import io.deephaven.util.annotations.FinalDefault;

import javax.annotation.concurrent.Immutable;

/**
 * Sub-interface of {@link TableLocationKey} to mark immutable implementations.
 */
@Immutable
public interface ImmutableTableLocationKey extends TableLocationKey {

    ImmutableTableLocationKey[] ZERO_LENGTH_IMMUTABLE_TABLE_LOCATION_KEY_ARRAY = new ImmutableTableLocationKey[0];

    @FinalDefault
    default ImmutableTableLocationKey makeImmutable() {
        return this;
    }
}
