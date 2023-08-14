package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Instant;

/**
 * Created by rbasralian on 10/19/22
 */
public class StreamJsonMessage extends BaseMessageMetadata implements JsonMessage {

    private final InputStream inputStream;
    private final Runnable afterParseAction;

    /**
     * Create a new instance of this class.
     *
     * @param sentTime The time (if available) when this message was sent
     * @param receiveTime The time (reported by subscriber) when this message was received.
     * @param ingestTime The time when this message was finished processing by its ingester and was ready to be flushed.
     * @param messageId An optional message ID string. (Used by some message brokers to support recovery.)
     * @param messageNumber The unique, monotonically-increasing sequential number for this message.
     * @param inputStream The stream containing the message body. The stream will be closed after it is consumed.
     * @param afterParseAction Operation to run after parsing the JSON and closing the input stream (e.g. to close an
     *        HTTP response).
     */
    public StreamJsonMessage(Instant sentTime, Instant receiveTime, Instant ingestTime, String messageId,
            long messageNumber, InputStream inputStream, Runnable afterParseAction) {
        super(sentTime, receiveTime, ingestTime, messageId, messageNumber);
        this.inputStream = inputStream;
        this.afterParseAction = afterParseAction;
    }

    @Override
    public JsonNode getJson() throws JsonNodeUtil.JsonStringParseException {
        final JsonNode jsonNode;
        try {
            try {
                jsonNode = JsonNodeUtil.makeJsonNode(inputStream);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    new UncheckedIOException("Failed closing input stream", e).printStackTrace();
                }
            }
        } finally {
            if (afterParseAction != null) {
                afterParseAction.run();
            }
        }

        return jsonNode;
    }

}
