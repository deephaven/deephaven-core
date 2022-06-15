/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

public interface HasTicketId {

    /**
     * Get the ticket ID.
     *
     * @return the ticket ID
     */
    TicketId ticketId();
}
