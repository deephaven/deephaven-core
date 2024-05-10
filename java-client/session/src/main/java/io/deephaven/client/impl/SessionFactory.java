//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.grpc.ManagedChannel;

public interface SessionFactory {
    /**
     * Creates a new {@link Session}. Closing the session does <b>not</b> close the {@link #managedChannel()}.
     *
     * @return the new session
     */
    Session newSession();

    /**
     * The {@link ManagedChannel} associated with {@code this} factory. Use {@link ManagedChannel#shutdown()} when
     * {@code this} factory and sessions are no longer needed.
     *
     * @return the managed channel
     */
    ManagedChannel managedChannel();
}
