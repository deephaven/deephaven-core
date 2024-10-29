//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;


import org.apache.arrow.flight.impl.Flight.FlightDescriptor;

/**
 * A specialization of {@link TicketResolver} that signifies this resolver supports Flight descriptor commands.
 *
 * <p>
 * Unfortunately, there is no universal way to know whether a command belongs to a given Flight protocol or not; at
 * best, we can assume (or mandate) that all the supportable command bytes are sufficiently unique such that there is no
 * potential for overlap amongst the installed Flight protocols.
 *
 * <p>
 * For example there could be command protocols built on top of Flight that simply use integer ordinals as their command
 * serialization format. In such a case, only one such protocol could safely be installed; otherwise, there would be no
 * reliable way of differentiating between them from the command bytes. (It's possible that other means of
 * differentiating could be established, like header values.)
 *
 * <p>
 * If Deephaven is in a position to create a protocol that uses Flight commands, or advise on their creation, it would
 * probably be wise to use a command serialization format that has a "unique" magic value as its prefix.
 *
 * <p>
 * The Flight SQL approach is to use the protobuf message Any to wrap up the respective protobuf Flight SQL command
 * message. While this approach is very likely to produce a sufficiently unique selection criteria, it requires
 * "non-trivial" parsing to determine whether the command is supported or not.
 */
public interface CommandResolver extends TicketResolver {

    /**
     * Returns {@code true} if this resolver is responsible for handling the {@code descriptor} command. Implementations
     * should prefer to return {@code true} here if they know the command is in their domain even if they don't
     * implement it; this allows them to provide a more specific error message for unsupported commands.
     *
     * @param descriptor the descriptor
     * @return {@code true} if this resolver handles the descriptor command
     */
    boolean handlesCommand(FlightDescriptor descriptor);
}
