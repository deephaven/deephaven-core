package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A message comprising JSON content and {@link MessageMetadata metadata}.
 */
public interface JsonMessage extends MessageMetadata {

    JsonNode getJson() throws JsonNodeUtil.JsonStringParseException;

}
