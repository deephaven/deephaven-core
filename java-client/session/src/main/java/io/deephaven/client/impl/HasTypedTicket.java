/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

public interface HasTypedTicket extends HasTicketId {

    /**
     * Get the typed ticket.
     *
     * @return the typed ticket
     */
    TypedTicket typedTicket();
}
