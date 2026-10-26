//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.state;

/**
 * A table or retainer that must re-establish its own server-side resources after the {@link ClientTableState} it
 * depends on has been revived following a reconnect.
 */
public interface Revivable {
    void revive(ClientTableState state);

    /**
     * The state could not be revived; this object cannot reconnect and should report that to its holder.
     */
    void die(Object failure);
}
