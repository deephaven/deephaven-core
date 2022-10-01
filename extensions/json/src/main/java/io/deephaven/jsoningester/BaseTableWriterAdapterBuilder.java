/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.TableWriter;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Create a builder for processing message payloads. Common properties used by different message to table writer adapters.
 */
public abstract class BaseTableWriterAdapterBuilder<M> {

    String sendTimestampColumnName;
    String receiveTimestampColumnName;
    String timestampColumnName;
    String messageIdColumnName;

    /**
     * Sets the name of the message publication timestamp column in the output table.
     * <p>
     * If this is set, then the output column will contain a send timestamp as reported by the subscriber.
     *
     * @param sendTimestampColumnName name of the publication column
     * @return this builder
     */
    public BaseTableWriterAdapterBuilder<M> sendTimestampColumnName(final String sendTimestampColumnName) {
        this.sendTimestampColumnName = sendTimestampColumnName;
        return this;
    }

    /**
     * Sets the name of the receive timestamp column in the output table.
     * <p>
     * If this is set, then the output column will contain a timestamp as reported by the subscriber.
     *
     * @param receiveTimestampColumnName name of the receive timestamp column
     * @return this builder
     */
    public BaseTableWriterAdapterBuilder<M> receiveTimestampColumnName(final String receiveTimestampColumnName) {
        this.receiveTimestampColumnName = receiveTimestampColumnName;
        return this;
    }

    /**
     * Sets the name of the ingestion timestamp column in the output table.
     * <p>
     * If this is set, then the output column will contain an ingestion timestamp that is the wall-clock time when
     * the record was consumed by the adapter.
     *
     * @param timestampColumnName name of the ingestion timestamp column
     * @return this builder
     */
    public BaseTableWriterAdapterBuilder<M> timestampColumnName(final String timestampColumnName) {
        this.timestampColumnName = timestampColumnName;
        return this;
    }

    /**
     * Set the name of the message ID column in the output table.
     * <p>
     * If this is set, then the output column will contain the message ID header value. The message ID is a String
     * header defined within messages, used internally by Deephaven to resume processing a queue from
     * the last checkpointed position. You may include it in the output table explicitly for debugging purposes.
     *
     * @param messageIdColumnName name of the message ID column
     * @return this builder
     */
    public BaseTableWriterAdapterBuilder<M> messageIdColumnName(final String messageIdColumnName) {
        this.messageIdColumnName = messageIdColumnName;
        return this;
    }

    /**
     * Return a list of internally used column names.
     *
     * @return the list of defined internal columns (timestamps or message ID)
     */
    @SuppressWarnings("WeakerAccess")
    protected Collection<String> getInternalColumns() {
        return Stream.of(receiveTimestampColumnName, sendTimestampColumnName, timestampColumnName, messageIdColumnName).filter(Objects::nonNull).collect(Collectors.toList());
    }

    public abstract Function<TableWriter<?>, MessageToTableWriterAdapter<M>> buildFactory(final Logger log);
}
