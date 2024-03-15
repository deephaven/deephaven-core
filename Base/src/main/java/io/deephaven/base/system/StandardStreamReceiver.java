//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.system;

import java.io.OutputStream;
import java.util.Optional;

/**
 * Allows classes to register interest in receiving application level calls to {@link System#out} and
 * {@link System#err}.
 */
public interface StandardStreamReceiver {

    /**
     * Registers interest in {@link System#out}.
     *
     * @return if present, represents interest in receiving {@link System#out} calls.
     */
    Optional<OutputStream> receiveOut();

    /**
     * Registers interest in {@link System#err}.
     *
     * @return if present, represents interest in receiving {@link System#err} calls.
     */
    Optional<OutputStream> receiveErr();
}
