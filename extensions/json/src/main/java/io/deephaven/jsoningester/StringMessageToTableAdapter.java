/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Translates a message into a standardized form for further processing, including attaching any needed metadata.
 */
public class StringMessageToTableAdapter<M> implements MessageToTableWriterAdapter<M> {

    private final StringToTableWriterAdapter stringAdapter;
    private final String messageIdColumn;
    private final String sendTimeColumn;
    private final String receiveTimeColumn;
    private final String nowTimeColumn;
    private final RowSetter<String> messageIdSetter;
    private final RowSetter<Instant> nowSetter;
    private final RowSetter<Instant> sendTimeSetter;
    private final RowSetter<Instant> receiveTimeSetter;
    private final AtomicLong messageNumber = new AtomicLong(0);

    private final Function<M, String> messageToText;
    private final ToLongFunction<M> messageToSendTimeMicros;
    private final ToLongFunction<M> messageToRecvTimeMicros;

    private StringMessageToTableAdapter(final TableWriter<?> tableWriter,
            final String sendTimeColumn,
            final String receiveTimeColumn,
            final String nowTimeColumn,
            final String messageIdColumn,
            final StringToTableWriterAdapter stringAdapter,
            Function<M, String> messageToText,
            ToLongFunction<M> messageToSendTimeMicros,
            ToLongFunction<M> messageToRecvTimeMicros) {
        this.stringAdapter = stringAdapter;
        stringAdapter.setOwner(this);
        this.messageIdColumn = messageIdColumn;
        this.sendTimeColumn = sendTimeColumn;
        this.receiveTimeColumn = receiveTimeColumn;
        this.nowTimeColumn = nowTimeColumn;
        if (sendTimeColumn != null) {
            sendTimeSetter = tableWriter.getSetter(sendTimeColumn, Instant.class);
        } else {
            sendTimeSetter = null;
        }
        if (receiveTimeColumn != null) {
            receiveTimeSetter = tableWriter.getSetter(receiveTimeColumn, Instant.class);
        } else {
            receiveTimeSetter = null;
        }
        if (nowTimeColumn != null) {
            nowSetter = tableWriter.getSetter(nowTimeColumn, Instant.class);
        } else {
            nowSetter = null;
        }
        if (messageIdColumn != null) {
            messageIdSetter = tableWriter.getSetter(messageIdColumn, String.class);
        } else {
            messageIdSetter = null;
        }

        this.messageToText = messageToText;
        this.messageToSendTimeMicros = messageToSendTimeMicros;
        this.messageToRecvTimeMicros = messageToRecvTimeMicros;
    }

    @Override
    public String getLastMessageId() {
        return null;
    }

    @Override
    public void consumeMessage(final String msgId, final M msg) throws IOException {
        final String msgText = messageToText.apply(msg);
        Instant sentTime = null;
        Instant receiveTime = null;
        Instant ingestTime = null;

        if (sendTimeSetter != null) {
            final long sendTimeMicros = messageToSendTimeMicros.applyAsLong(msg);
            // Ignore non-positive timestamps. In practice, NULL_LONG or 0 may occur here to indicate "nothing".
            // Any other negative value is nonsense.
            if (sendTimeMicros > 0) {
                sentTime = DateTimeUtils.epochMicrosToInstant(sendTimeMicros);
            }
            // do not set the value here; let the StringToTableWriterAdapter handle it, in case there are multiple
            // threads
        }
        if (receiveTimeSetter != null) {
            final long recvTimeMicros = messageToRecvTimeMicros.applyAsLong(msg);
            // Ignore non-positive timestamps. In practice, NULL_LONG or 0 may occur here to indicate "nothing".
            // Any other negative value is nonsense.
            if (recvTimeMicros > 0) {
                receiveTime = DateTimeUtils.epochMicrosToInstant(recvTimeMicros);
            }
            // do not set the value here; let the StringToTableWriterAdapter handle it, in case there are multiple
            // threads

        }
        if (nowSetter != null) {
            ingestTime = Instant.now();
            // do not set the value here; let the StringToTableWriterAdapter handle it, in case there are multiple
            // threads
        }

        final TextMessage metadata = new TextMessage(sentTime, receiveTime, ingestTime, msgId,
                messageNumber.getAndIncrement(), msgText);

        stringAdapter.consumeString(metadata);
    }

    protected void setMetadataSetters() {

    }

    public String getMessageIdColumn() {
        return messageIdColumn;
    }

    public String getSendTimeColumn() {
        return sendTimeColumn;
    }

    public String getReceiveTimeColumn() {
        return receiveTimeColumn;
    }

    @ScriptApi
    public String getNowTimeColumn() {
        return nowTimeColumn;
    }

    public RowSetter<Instant> getSendTimeSetter() {
        return sendTimeSetter;
    }

    public void setSendTime(Instant sendTime) {
        if (sendTimeSetter != null) {
            sendTimeSetter.set(sendTime);
        }
    }

    public RowSetter<Instant> getReceiveTimeSetter() {
        return receiveTimeSetter;
    }

    public void setReceiveTime(Instant receiveTime) {
        if (receiveTimeSetter != null) {
            receiveTimeSetter.set(receiveTime);
        }
    }

    public RowSetter<Instant> getNowSetter() {
        return nowSetter;
    }

    public void setNow(Instant getNow) {
        if (nowSetter != null) {
            nowSetter.set(getNow);
        }
    }

    public RowSetter<String> getMessageIdSetter() {
        return messageIdSetter;
    }

    public void setMessageId(String messageId) {
        if (messageIdSetter != null) {
            messageIdSetter.set(messageId);
        }
    }

    @Override
    public void cleanup() throws IOException {
        stringAdapter.cleanup();
    }

    @Override
    public void shutdown() {
        stringAdapter.shutdown();
    }

    // @Override
    // public void setProcessor(@NotNull final SimpleDataImportStreamProcessor processor,
    // final String lastCheckpointId) {
    // return; // Not currently used for JSON message processing
    // }

    @Override
    public void waitForProcessing(final long timeoutMillis) throws InterruptedException, TimeoutException {
        stringAdapter.waitForProcessing(timeoutMillis);
    }

    public abstract static class Builder<A extends StringToTableWriterAdapter>
            extends BaseTableWriterAdapterBuilder<A> {

        @Deprecated
        public Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> buildFactory(Logger log) {
            return StringMessageToTableAdapter.buildFactory(log, this);
        }
    }

    /**
     * Returns a factory that creates adapters that take messages of type {@code M}, unpack the message text and
     * timestamps, and pass the message data to an adapter created from the given {@code adapterBuilder} (e.g. a
     * {@link JSONToTableWriterAdapterBuilder}).
     * <p>
     * This is helpful when creating multiple for different partitions.
     *
     * @param log The logger
     * @param adapterBuilder Adapter builder
     * @param messageToText Function to extract text data from an instance of type {@code M}
     * @param messageToSendTimeMicros Function to extract a send timestamp from an instance of type {@code M}
     * @param messageToRecvTimeMicros Function to extract a receipt timestamp from an instance of type {@code M}
     * @param <M> The message datatype
     * @return A function that takes a TableWriter and returns a new {@code StringMessageToTableAdapter} that writes
     *         data to that TableWriter.
     */
    public static <M> Function<TableWriter<?>, StringMessageToTableAdapter<M>> buildFactory(
            @NotNull final Logger log,
            @NotNull final BaseTableWriterAdapterBuilder<? extends StringToTableWriterAdapter> adapterBuilder,
            @NotNull final Function<M, String> messageToText,
            @NotNull final ToLongFunction<M> messageToSendTimeMicros,
            @NotNull final ToLongFunction<M> messageToRecvTimeMicros) {
        return (tw) -> {
            // create the string-to-tablewriter adapter
            final StringToTableWriterAdapter stringToTableWriterAdapter = adapterBuilder.makeAdapter(log, tw);

            // create a message-to-tablewriter adapter, which runs the message content through the string-to-tablewriter
            // adapter
            return new StringMessageToTableAdapter<>(tw,
                    adapterBuilder.sendTimestampColumnName,
                    adapterBuilder.receiveTimestampColumnName,
                    adapterBuilder.timestampColumnName,
                    adapterBuilder.messageIdColumnName,
                    stringToTableWriterAdapter,
                    messageToText,
                    messageToSendTimeMicros,
                    messageToRecvTimeMicros);
        };
    }

    public static Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> buildFactory(
            @NotNull final Logger log,
            @NotNull final BaseTableWriterAdapterBuilder<? extends StringToTableWriterAdapter> adapterBuilder) {
        return buildFactory(
                log,
                adapterBuilder,
                StringMessageHolder::getMsg,
                StringMessageHolder::getSendTimeMicros,
                StringMessageHolder::getRecvTimeMicros);
    }

    public static BiFunction<TableWriter<?>, Map<String, TableWriter<?>>, StringMessageToTableAdapter<StringMessageHolder>> buildFactoryWithSubtables(
            Logger log, JSONToTableWriterAdapterBuilder adapterBuilder) {
        return (tablewriter, subtableWritersMap) -> {
            // create the string-to-tablewriter adapter
            final StringToTableWriterAdapter stringToTableWriterAdapter =
                    adapterBuilder.makeAdapter(log, tablewriter, subtableWritersMap);

            // create a message-to-tablewriter adapter, which runs the message content through the string-to-tablewriter
            // adapter
            return new StringMessageToTableAdapter<>(tablewriter,
                    adapterBuilder.sendTimestampColumnName,
                    adapterBuilder.receiveTimestampColumnName,
                    adapterBuilder.timestampColumnName,
                    adapterBuilder.messageIdColumnName,
                    stringToTableWriterAdapter,
                    StringMessageHolder::getMsg,
                    StringMessageHolder::getSendTimeMicros,
                    StringMessageHolder::getRecvTimeMicros);
        };
    }

}
