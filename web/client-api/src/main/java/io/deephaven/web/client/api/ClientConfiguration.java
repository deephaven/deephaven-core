/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;

/**
 * A place to assemble various "services" we want to make ubiquitously available in the client by passing around a
 * single object.
 */
public class ClientConfiguration {
    private static final byte EXPORT_PREFIX = 'e';

    /**
     * The next number to use when making a ticket. These values must always be positive, as zero is an invalid value,
     * and negative values represent server-created tickets.
     */
    private int next = 1;

    public ClientConfiguration() {}

    public Ticket newTicket() {
        Ticket ticket = new Ticket();
        ticket.setTicket(newTicketRaw());
        return ticket;
    }

    public int newTicketInt() {
        return next++;
    }

    public Uint8Array newTicketRaw() {
        if (next == Integer.MAX_VALUE) {
            throw new IllegalStateException("Ran out of tickets!");
        }

        final int exportId = next++;
        final double[] dest = new double[5];
        dest[0] = EXPORT_PREFIX;
        dest[1] = (byte) exportId;
        dest[2] = (byte) (exportId >>> 8);
        dest[3] = (byte) (exportId >>> 16);
        dest[4] = (byte) (exportId >>> 24);

        final Uint8Array bytes = new Uint8Array(5);
        bytes.set(dest);
        return bytes;
    }
}
