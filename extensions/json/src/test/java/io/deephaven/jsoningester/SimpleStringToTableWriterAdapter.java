/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Instant;

/**
 * Converts String messages directly into a string column of data in the output table.
 */
public class SimpleStringToTableWriterAdapter implements StringToTableWriterAdapter {
    private final TableWriter<?> writer;
    @NotNull
    private final RowSetter<String> valueColumnSetter;
    private StringMessageToTableAdapter<?> owner;

    private SimpleStringToTableWriterAdapter(final TableWriter<?> writer,
            final String valueColumnName) {
        this.writer = writer;
        this.valueColumnSetter = writer.getSetter(valueColumnName, String.class);
    }


    @Override
    public void consumeString(final TextMessage msg) throws IOException {
        valueColumnSetter.set(msg.getText());

        owner.setSendTime(msg.getSentTime());
        owner.setReceiveTime(msg.getReceiveTime());
        owner.setNow(Instant.now());
        owner.setMessageId(msg.getMessageId());

        writer.writeRow();
    }

    @Override
    public void cleanup() {
        // No action needed for cleanup.
    }

    @Override
    public void shutdown() {
        // No action needed for shutdown.
    }

    @Override
    public void waitForProcessing(final long timeoutMillis) {
        // no-op
    }

    @Override
    public void setOwner(final StringMessageToTableAdapter<?> parent) {
        this.owner = parent;
    }

    /**
     * Create a builder for processing message payloads as simple strings.
     */
    public static class Builder extends StringMessageToTableAdapter.Builder<SimpleStringToTableWriterAdapter> {
        String valueColumnName;

        /**
         * Set the name of the value column in the output table.
         * <p>
         * The output column contains the string payload of the message and must be defined.
         *
         * @param valueColumnName name of the output column
         * @return this builder
         */
        public Builder setValueColumnName(final String valueColumnName) {
            this.valueColumnName = valueColumnName;
            return this;
        }

        @Override
        public SimpleStringToTableWriterAdapter makeAdapter(Logger log, TableWriter<?> tw) {
            if (valueColumnName == null) {
                throw new IllegalArgumentException("Value column must be specified!");
            }
            return new SimpleStringToTableWriterAdapter(tw, valueColumnName);
        }

    }
}
