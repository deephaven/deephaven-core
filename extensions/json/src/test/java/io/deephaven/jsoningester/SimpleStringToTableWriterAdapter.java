/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.function.Function;

/**
 * Converts String messages directly into a string column of data in the output table.
 */
public class SimpleStringToTableWriterAdapter implements StringToTableWriterAdapter {
    private final TableWriter<?> writer;
    @NotNull
    private final RowSetter<String> valueColumnSetter;

    private SimpleStringToTableWriterAdapter(final TableWriter<?> writer,
                                             final String valueColumnName) {
        this.writer = writer;
        this.valueColumnSetter = writer.getSetter(valueColumnName, String.class);
    }


    @Override
    public void consumeString(final TextMessageMetadata metadata,
                              final String input) throws IOException {
        valueColumnSetter.set(input);
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
        // Do nothing; this adapter doesn't care.
    }

    /**
     * Create a builder for processing message payloads as simple strings.
     */
    public static class Builder extends StringMessageToTableAdapter.Builder<StringMessageToTableAdapter<TextMessage>> {
        String valueColumnName;

        /**
         * Set the name of the value column in the output table.
         *
         * The output column contains the string payload of the message and must be defined.
         *
         * @param valueColumnName name of the output column
         *
         * @return this builder
         */
        public Builder setValueColumnName(final String valueColumnName) {
            this.valueColumnName = valueColumnName;
            return this;
        }

        @Override
        public Function<TableWriter<?>, StringMessageToTableAdapter<TextMessage>> buildFactory(Logger log) {
            if (valueColumnName == null) {
                throw new IllegalArgumentException("Value column must be specified!");
            }

            return (tw) -> {
                final StringToTableWriterAdapter stringAdapter = new SimpleStringToTableWriterAdapter(tw, valueColumnName);
                return buildInternal(tw, stringAdapter,
                        StringMessageHolder::getMsg,
                        StringMessageHolder::getSendTimeMicros,
                        StringMessageHolder::getRecvTimeMicros);
            };
        }
    }
}