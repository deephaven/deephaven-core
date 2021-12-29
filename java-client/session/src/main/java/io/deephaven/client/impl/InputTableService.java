package io.deephaven.client.impl;

import java.util.concurrent.CompletableFuture;

public interface InputTableService {
    /**
     * Add {@code source} to the input table {@code destination}.
     *
     * @param destination the input table
     * @param source the source to add
     * @return the future
     */
    CompletableFuture<Void> addToInputTable(HasTicketId destination, HasTicketId source);

    /**
     * Delete {@code source} from the input table {@code destination}.
     *
     * @param destination the input table
     * @param source the source to delete
     * @return the future
     */
    CompletableFuture<Void> deleteFromInputTable(HasTicketId destination, HasTicketId source);
}
