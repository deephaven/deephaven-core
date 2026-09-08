//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.state;

import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.web.client.api.TableTicket;
import io.deephaven.web.client.state.ClientTableState;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A container for all known table states within the application.
 *
 * You should only remove entries from this cache when all JsTable who might reference a given state have abandoned said
 * state.
 *
 */
public class StateCache {

    private final Map<TableTicket, ClientTableState> allStates = new HashMap<>();

    public Optional<ClientTableState> get(Ticket ticket) {
        return get(new TableTicket(ticket));
    }

    public Optional<ClientTableState> get(TableTicket handle) {
        return Optional.ofNullable(allStates.get(handle));
    }

    public ClientTableState getNullable(Ticket handle) {
        return getNullable(new TableTicket(handle));
    }

    public ClientTableState getNullable(TableTicket handle) {
        return allStates.get(handle);
    }

    public ClientTableState create(TableTicket handle, Function<TableTicket, ClientTableState> factory) {
        if (handle.getState() != TableTicket.State.PENDING) {
            throw new IllegalStateException("Should be pending " + handle);
        }
        if (allStates.containsKey(handle)) {
            throw new IllegalStateException("already exists " + handle);
        }
        return allStates.computeIfAbsent(handle, factory);
    }

    public void release(ClientTableState state) {
        final ClientTableState was = allStates.remove(state.getHandle());
        assert was == null || was == state
                : "Released a state with the same handle but a different instance than expected";
    }

    public Collection<ClientTableState> getAllStates() {
        return allStates.values();
    }
}
