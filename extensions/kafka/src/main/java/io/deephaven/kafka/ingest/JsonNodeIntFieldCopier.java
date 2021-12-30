/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit JsonNodeCharFieldCopier and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableChunk;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.chunk.attributes.Values;

public class JsonNodeIntFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeIntFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk, final WritableChunk<Values> publisherChunk,
            final int sourceOffset, final int destOffset, final int length
    ) {
        final WritableIntChunk<Values> output = publisherChunk.asWritableIntChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            output.set(ii + destOffset, JsonNodeUtil.getInt(node, fieldName, true, true));
        }
    }
}
