package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A message comprising JSON content and {@link MessageMetadata metadata}.
 */
public interface JsonMessage extends MessageMetadata {

    JsonNode getJson() throws JsonNodeUtil.JsonStringParseException;

    /**
     * Returns the original text for the message.
     * 
     * @return The original message text, or {@code null} if it is not available.
     */
    default String getOriginalText() {
        return null;
    }

}
