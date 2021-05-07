package io.deephaven.web.client.api;

import elemental2.core.Int32Array;
import elemental2.core.Uint8Array;

/**
 * A place to assemble various "services" we want to make
 * ubiquitously available in the client by passing around a single object.
 */
public class ClientConfiguration {

    /**
     * The next number to use when making a ticket. These values must always be positive, as zero
     * is an invalid value, and negative values represent server-created tickets.
     */
    private int next = 1;

    public ClientConfiguration() {
    }

    public Uint8Array newTicket() {
        // THe current implementation takes the simplest approach of assuming that we can't have more than
        // Integer.MAX_VALUE tickets in a session.
        if (next == Integer.MAX_VALUE) {
            throw new IllegalStateException("Ran out of tickets!");
        }
        Int32Array ints = new Int32Array(2);
        ints.set(new double[] {next++, 0});
        return new Uint8Array(ints.buffer);
    }
}
