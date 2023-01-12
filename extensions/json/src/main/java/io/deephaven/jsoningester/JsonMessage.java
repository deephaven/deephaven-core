package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A message comprising JSON content and {@link MessageMetadata metadata}.
 */
public interface JsonMessage extends MessageMetadata {

    /**
     * Parses and returns the JSON content of this message.
     * 
     * @return A {@code JsonNode} parsed from this message.
     * @throws JsonNodeUtil.JsonStringParseException If an exception occurs while parsing the JSON content.
     */
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
