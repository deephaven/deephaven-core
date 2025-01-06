//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;


import org.apache.arrow.flight.impl.Flight.FlightDescriptor;

/**
 * A specialization of {@link TicketResolver} that signifies this resolver supports Flight descriptor paths.
 */
public interface PathResolver extends TicketResolver {

    /**
     * Returns {@code true} if this resolver is responsible for handling the {@code descriptor} path. Implementations
     * should prefer to return {@code true} here if they know the path is in their domain even if they don't implement
     * it; this allows them to provide a more specific error message for unsupported paths.
     *
     * @param descriptor the descriptor
     * @return {@code true} if this resolver handles the descriptor path
     */
    boolean handlesPath(FlightDescriptor descriptor);
}
