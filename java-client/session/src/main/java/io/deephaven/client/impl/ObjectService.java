package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface ObjectService {
    /**
     * Fetch the object.
     *
     * @param ticket the ticket
     * @return the future
     */
    CompletableFuture<FetchedObject> fetchObject(HasTicketId ticket);
}
