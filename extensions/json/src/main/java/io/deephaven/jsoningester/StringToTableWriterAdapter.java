/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import java.io.IOException;

/**
 * String messages must be adapted to a TableWriter. The StringToTableWriterAdapter consumes a String message and writes
 * zero or more rows to a TableWriter.
 */
public interface StringToTableWriterAdapter extends DataToTableWriterAdapter {
    /**
     * Consume a generic String and write zero or more records to a TableWriter.
     *
     * @param metadata The metadata associated with the string to be consumed. ID must be unique and increasing for any
     *        source using this method to write data to a Deephaven database.
     * @throws IOException if there was an error writing to the output table
     */
    void consumeString(final TextMessageMetadata metadata) throws IOException;

    /**
     * Record the owning StringMessageToTableAdapter, if this StringToTableWriterAdapter needs that.
     * 
     * @param parent The StringMessageToTableAdapter that controls this StringToTableWriterAdapter.
     */
    void setOwner(StringMessageToTableAdapter<?> parent);

}
