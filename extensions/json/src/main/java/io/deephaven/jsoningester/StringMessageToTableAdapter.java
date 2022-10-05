/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.io.logger.Logger;
import io.deephaven.tablelogger.RowSetter;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.time.DateTime;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Translates a message into a standardized form for further processing, including attaching any needed metadata.
 */
public class StringMessageToTableAdapter<M> implements MessageToTableWriterAdapter<M> {
    private static final long MILLIS_TO_NANOS = 1_000_000L;

    private final StringToTableWriterAdapter stringAdapter;
    private final String messageIdColumn;
    private final String sendTimeColumn;
    private final String receiveTimeColumn;
    private final String nowTimeColumn;
    private final RowSetter<String> messageIdSetter;
    private final RowSetter<DateTime> nowSetter;
    private final RowSetter<DateTime> sendTimeSetter;
    private final RowSetter<DateTime> receiveTimeSetter;
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
            sendTimeSetter = tableWriter.getSetter(sendTimeColumn, DateTime.class);
        } else {
            sendTimeSetter = null;
        }
        if (receiveTimeColumn != null) {
            receiveTimeSetter = tableWriter.getSetter(receiveTimeColumn, DateTime.class);
        } else {
            receiveTimeSetter = null;
        }
        if (nowTimeColumn != null) {
            nowSetter = tableWriter.getSetter(nowTimeColumn, DateTime.class);
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
        DateTime sentTime = null;
        DateTime receiveTime = null;
        DateTime ingestTime = null;

        if (sendTimeSetter != null) {
            final long senderTimestamp = messageToSendTimeMicros.applyAsLong(msg);
            if (senderTimestamp != 0) {
                sentTime = new DateTime(senderTimestamp * MILLIS_TO_NANOS);
            }
            sendTimeSetter.set(sentTime);
        }
        if (receiveTimeSetter != null) {
            final long timestamp = messageToRecvTimeMicros.applyAsLong(msg);
            if (timestamp != 0) {
                receiveTime = new DateTime(timestamp * MILLIS_TO_NANOS);
            }
            receiveTimeSetter.set(receiveTime);

        }
        if (nowSetter != null) {
            ingestTime = DateTime.now();
            nowSetter.set(ingestTime);
        }
        if (messageIdSetter != null) {
            messageIdSetter.set(msgId);
        }
        final TextMessageMetadata metadata = new TextMessageMetadata(sentTime, receiveTime, ingestTime, msgId,
                messageNumber.getAndIncrement(), msgText);

        stringAdapter.consumeString(metadata);
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

    public RowSetter<DateTime> getSendTimeSetter() {
        return sendTimeSetter;
    }

    public RowSetter<DateTime> getReceiveTimeSetter() {
        return receiveTimeSetter;
    }

    public RowSetter<DateTime> getNowSetter() {
        return nowSetter;
    }

    public RowSetter<String> getMessageIdSetter() {
        return messageIdSetter;
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
     * @param messageToText Function to extract text data from {@code M}
     * @param messageToSendTimeMicros Function to extract a send timestamp from {@code M}
     * @param messageToRecvTimeMicros Function to extract a receipt timestamp from {@code M}
     * @param <M>
     * @return A function that takes a TableWriter and returns a new {@code StringMessageToTableAdapter} that writes
     *         data to that TableWriter.
     */
    public static <M> Function<TableWriter<?>, StringMessageToTableAdapter<M>> buildFactory(
            @NotNull final Logger log,
            @NotNull final BaseTableWriterAdapterBuilder<? extends StringToTableWriterAdapter> adapterBuilder,
            @NotNull final Function<M, String> messageToText,
            @NotNull final ToLongFunction<M> messageToSendTimeMicros,
            @NotNull final ToLongFunction<M> messageToRecvTimeMicros) {
        return (tw) -> new StringMessageToTableAdapter<>(tw,
                adapterBuilder.sendTimestampColumnName,
                adapterBuilder.receiveTimestampColumnName,
                adapterBuilder.timestampColumnName,
                adapterBuilder.messageIdColumnName,
                adapterBuilder.makeAdapter(log, tw),
                messageToText,
                messageToSendTimeMicros,
                messageToRecvTimeMicros);
    }

    public static Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> buildFactory(
            @NotNull final Logger log,
            @NotNull final BaseTableWriterAdapterBuilder<? extends StringToTableWriterAdapter> adapterBuilder) {
        return (tw) -> new StringMessageToTableAdapter<>(tw,
                adapterBuilder.sendTimestampColumnName,
                adapterBuilder.receiveTimestampColumnName,
                adapterBuilder.timestampColumnName,
                adapterBuilder.messageIdColumnName,
                adapterBuilder.makeAdapter(log, tw),
                StringMessageHolder::getMsg,
                StringMessageHolder::getSendTimeMicros,
                StringMessageHolder::getRecvTimeMicros);
    }
}
