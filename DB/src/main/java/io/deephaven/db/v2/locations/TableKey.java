package io.deephaven.db.v2.locations;

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
