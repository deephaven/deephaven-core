/*
 * Copyright (c) 2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.io.logger.Logger;
import io.deephaven.jsoningester.msg.TextMessage;
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
public class StringMessageToBlinkTableAdapter<M> implements MessageToIngesterAdapter<M> {

    private final StringIngestionAdapter stringAdapter;
    private final String messageIdColumn;
    private final String sendTimeColumn;
    private final String receiveTimeColumn;
    private final String nowTimeColumn;
    private final AtomicLong messageNumber = new AtomicLong(0);
    private final Function<M, String> messageToText;
    private final ToLongFunction<M> messageToSendTimeMicros;
    private final ToLongFunction<M> messageToRecvTimeMicros;

    private StringMessageToBlinkTableAdapter(final SimpleStreamPublisher streamPublisher,
                                             final String sendTimeColumn,
                                             final String receiveTimeColumn,
                                             final String nowTimeColumn,
                                             final String messageIdColumn,
                                             final StringIngestionAdapter<StringMessageToBlinkTableAdapter<M>> stringAdapter,
                                             Function<M, String> messageToText,
                                             ToLongFunction<M> messageToSendTimeMicros,
                                             ToLongFunction<M> messageToRecvTimeMicros) {
        this.stringAdapter = stringAdapter;
        this.messageIdColumn = messageIdColumn;
        this.sendTimeColumn = sendTimeColumn;
        this.receiveTimeColumn = receiveTimeColumn;
        this.nowTimeColumn = nowTimeColumn;

        final TableDefinition tableDefinition = streamPublisher.getTableDefinition();
        if (sendTimeColumn != null) {
            tableDefinition.checkHasColumn(sendTimeColumn, Instant.class);
        }
        if (receiveTimeColumn != null) {
            tableDefinition.checkHasColumn(receiveTimeColumn, Instant.class);
        }
        if (nowTimeColumn != null) {
            tableDefinition.checkHasColumn(nowTimeColumn, Instant.class);
        }
        if (messageIdColumn != null) {
            tableDefinition.checkHasColumn(messageIdColumn, String.class);
        }

        // TODO: instead of setOwner(), seems like it would make more sense to pass this adapter to the StringIngestionAdapter,
        //   so this stuff could be handled in its constructor
        stringAdapter.setOwner(this);

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

        if (sendTimeColumn != null) {
            final long sendTimeMicros = messageToSendTimeMicros.applyAsLong(msg);
            // Ignore non-positive timestamps. In practice, NULL_LONG or 0 may occur here to indicate "nothing".
            // Any other negative value is nonsense.
            if (sendTimeMicros > 0) {
                sentTime = DateTimeUtils.epochMicrosToInstant(sendTimeMicros);
            }
            // do not set the value here; let the StringIngestionAdapter handle it, in case there are multiple
            // threads
        }
        if (receiveTimeColumn != null) {
            final long recvTimeMicros = messageToRecvTimeMicros.applyAsLong(msg);
            // Ignore non-positive timestamps. In practice, NULL_LONG or 0 may occur here to indicate "nothing".
            // Any other negative value is nonsense.
            if (recvTimeMicros > 0) {
                receiveTime = DateTimeUtils.epochMicrosToInstant(recvTimeMicros);
            }
            // do not set the value here; let the StringIngestionAdapter handle it, in case there are multiple
            // threads

        }
        if (nowTimeColumn != null) {
            ingestTime = Instant.now();
            // do not set the value here; let the StringIngestionAdapter handle it, in case there are multiple
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

    public abstract static class Builder<A extends StringIngestionAdapter>
            extends BaseStreamPublisherAdapterBuilder<A> {

        @Deprecated
        public Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> buildFactory(Logger log) {
            return StringMessageToBlinkTableAdapter.buildFactory(log, this);
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
     * @return A function that takes a TableWriter and returns a new {@code StringMessageToBlinkTableAdapter} that writes
     *         data to that TableWriter.
     */
    public static <M> Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<M>> buildFactory(
            @NotNull final Logger log,
            @NotNull final BaseStreamPublisherAdapterBuilder<? extends StringIngestionAdapter> adapterBuilder,
            @NotNull final Function<M, String> messageToText,
            @NotNull final ToLongFunction<M> messageToSendTimeMicros,
            @NotNull final ToLongFunction<M> messageToRecvTimeMicros) {
        return (sp) -> {
            // create the string-to-tablewriter adapter
            final StringIngestionAdapter stringIngestionAdapter = adapterBuilder.makeAdapter(log, sp);

            // create a message-to-blinktable adapter, which runs the message content through the string-to-blinktable
            // adapter
            return new StringMessageToBlinkTableAdapter<>(sp,
                    adapterBuilder.sendTimestampColumnName,
                    adapterBuilder.receiveTimestampColumnName,
                    adapterBuilder.timestampColumnName,
                    adapterBuilder.messageIdColumnName,
                    stringIngestionAdapter,
                    messageToText,
                    messageToSendTimeMicros,
                    messageToRecvTimeMicros);
        };
    }

    public static Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> buildFactory(
            @NotNull final Logger log,
            @NotNull final BaseStreamPublisherAdapterBuilder<? extends StringIngestionAdapter> adapterBuilder) {
        return buildFactory(
                log,
                adapterBuilder,
                StringMessageHolder::getMsg,
                StringMessageHolder::getSendTimeMicros,
                StringMessageHolder::getRecvTimeMicros);
    }

    public static BiFunction<SimpleStreamPublisher, Map<String, SimpleStreamPublisher>, StringMessageToBlinkTableAdapter<StringMessageHolder>> buildFactoryWithSubtables(
            Logger log, JSONToStreamPublisherAdapterBuilder adapterBuilder) {
        return (tablewriter, subtableWritersMap) -> {
            // create the string-to-tablewriter adapter
            final StringIngestionAdapter stringIngestionAdapter =
                    adapterBuilder.makeAdapter(log, tablewriter, subtableWritersMap);

            // create a message-to-tablewriter adapter, which runs the message content through the string-to-tablewriter
            // adapter
            return new StringMessageToBlinkTableAdapter<>(tablewriter,
                    adapterBuilder.sendTimestampColumnName,
                    adapterBuilder.receiveTimestampColumnName,
                    adapterBuilder.timestampColumnName,
                    adapterBuilder.messageIdColumnName,
                    stringIngestionAdapter,
                    StringMessageHolder::getMsg,
                    StringMessageHolder::getSendTimeMicros,
                    StringMessageHolder::getRecvTimeMicros);
        };
    }

//    public static BiFunction<TableWriter<?>, Map<String, TableWriter<?>>, StringMessageToBlinkTableAdapter<StringMessageHolder>> buildFactoryWithSubtables(
//            Logger log, JSONToStreamPublisherAdapterBuilder adapterBuilder) {
//
//
//        return (tablewriter, subtableWritersMap) -> {
//            // create the string-to-tablewriter adapter
//            final JSONToStreamPublisherAdapter stringToStreamPublisherAdapter =
//                    adapterBuilder.makeAdapter(log, tablewriter, subtableWritersMap);
//
//            // create a message-to-tablewriter adapter, which runs the message content through the string-to-tablewriter
//            // adapter
////            return new StringMessageToBlinkTableAdapter<>(tablewriter,
////                    adapterBuilder.sendTimestampColumnName,
////                    adapterBuilder.receiveTimestampColumnName,
////                    adapterBuilder.timestampColumnName,
////                    adapterBuilder.messageIdColumnName,
////                    stringToStreamPublisherAdapter,
////                    StringMessageHolder::getMsg,
////                    StringMessageHolder::getSendTimeMicros,
////                    StringMessageHolder::getRecvTimeMicros);
//
//
//            return new StreamToBlinkTableAdapter(
//                    tableDef,
//                    stringToStreamPublisherAdapter,
//                    updateSourceRegistrar,
//                    "TEST_ADAPTER" // TODO: use some arg
//            );
//        };
//    }

}
