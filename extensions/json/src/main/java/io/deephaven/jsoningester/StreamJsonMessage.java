package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * Created by rbasralian on 10/19/22
 */
public class StreamJsonMessage extends BaseMessageMetadata implements JsonMessage {

    private final InputStream inputStream;
    private final Runnable afterParse;

    /**
     * Create a new instance of this class.
     *
     * @param sentTime The time (if available) when this message was sent
     * @param receiveTime The time (reported by subscriber) when this message was received.
     * @param ingestTime The time when this message was finished processing by its ingester and was ready to be flushed.
     * @param messageId The unique, monotonically-increasing ID for this message.
     * @param messageNumber The sequential number indicating the sequence this message was received in by the ingester.
     * @param inputStream The stream containing the message body.
     * @param afterParse Runnable to run after closing the input stream (e.g. to close an http response)
     */
    public StreamJsonMessage(DateTime sentTime, DateTime receiveTime, DateTime ingestTime, String messageId,
            long messageNumber, InputStream inputStream, Runnable afterParse) {
        super(sentTime, receiveTime, ingestTime, messageId, messageNumber);
        this.inputStream = inputStream;
        this.afterParse = afterParse;
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
            if (afterParse != null) {
                afterParse.run();
            }
        }

        return jsonNode;
    }

}
