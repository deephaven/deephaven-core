package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * Created by rbasralian on 10/19/22
 */
public class TextJsonMessage extends TextMessage implements JsonMessage {

    /**
     * Create a new instance of this class.
     *
     * @param sentTime The time (if available) when this message was sent
     * @param receiveTime The time (reported by subscriber) when this message was received.
     * @param ingestTime The time when this message was finished processing by its ingester and was ready to be flushed.
     * @param messageId An optional message ID string. (Used by some message brokers to support recovery.)
     * @param messageNumber The monotonically-increasing sequence number for the message.
     * @param text The String message body.
     */
    public TextJsonMessage(Instant sentTime, Instant receiveTime, Instant ingestTime, String messageId,
            long messageNumber, String text) {
        super(sentTime, receiveTime, ingestTime, messageId, messageNumber, text);
    }

    @Override
    public JsonNode getJson() throws JsonNodeUtil.JsonStringParseException {
        return JsonNodeUtil.makeJsonNode(getText());
    }

}
