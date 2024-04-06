//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.type.NamedImplementation;

/**
 * Interface for opaque table keys for use in {@link TableDataService} implementations.
 */
public interface TableKey extends NamedImplementation, LogOutputAppendable {

    /**
     * Get an {@link ImmutableTableKey} that is equal to this.
     *
     * @return An immutable version of this key
     */
    ImmutableTableKey makeImmutable();
}
