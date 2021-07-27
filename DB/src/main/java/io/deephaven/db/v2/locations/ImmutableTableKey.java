package io.deephaven.db.v2.locations;

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
