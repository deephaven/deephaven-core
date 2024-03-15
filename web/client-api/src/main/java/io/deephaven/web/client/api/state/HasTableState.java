//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.state;

import io.deephaven.web.client.state.ClientTableState;

/**
 * An interface to allow you to put ActiveSubscription in the same collection as PausedSubscription
 */
public interface HasTableState<State extends ClientTableState> {

    State getState();

    boolean isActive();

    void rollback();
}
