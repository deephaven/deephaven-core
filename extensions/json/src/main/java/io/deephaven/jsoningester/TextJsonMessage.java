package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.time.DateTime;

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
     * @param messageId The unique, monotonically-increasing ID for this message.
     * @param messageNumber The sequential number indicating the sequence this message was received in by the ingester.
     * @param text The String message body.
     */
    public TextJsonMessage(DateTime sentTime, DateTime receiveTime, DateTime ingestTime, String messageId,
            long messageNumber, String text) {
        super(sentTime, receiveTime, ingestTime, messageId, messageNumber, text);
    }

    @Override
    public JsonNode getJson() throws JsonNodeUtil.JsonStringParseException {
        return JsonNodeUtil.makeJsonNode(getText());
    }

}
