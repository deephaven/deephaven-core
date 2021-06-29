package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.type.NamedImplementation;

/**
 * Interface for opaque table location keys for use in {@link TableLocationProvider} implementations.
 */
public interface TableLocationKey extends NamedImplementation, LogOutputAppendable {

    /**
     * Get an {@link ImmutableTableLocationKey} that is equal to this.
     *
     * @return An immutable version of this key
     */
    ImmutableTableLocationKey makeImmutable();
}
