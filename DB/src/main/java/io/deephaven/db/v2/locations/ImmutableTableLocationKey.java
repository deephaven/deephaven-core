package io.deephaven.db.v2.locations;

import io.deephaven.util.annotations.FinalDefault;

import javax.annotation.concurrent.Immutable;

/**
 * Sub-interface of {@link TableLocationKey} to mark immutable implementations.
 */
@Immutable
public interface ImmutableTableLocationKey extends TableLocationKey {

    @FinalDefault
    default ImmutableTableLocationKey makeImmutable() {
        return this;
    }
}
