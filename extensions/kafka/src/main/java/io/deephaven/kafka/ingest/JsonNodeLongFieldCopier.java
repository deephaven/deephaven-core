/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit JsonNodeCharFieldCopier and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableChunk;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.chunk.attributes.Values;

public class JsonNodeLongFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeLongFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk, final WritableChunk<Values> publisherChunk,
            final int sourceOffset, final int destOffset, final int length
    ) {
        final WritableLongChunk<Values> output = publisherChunk.asWritableLongChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            output.set(ii + destOffset, JsonNodeUtil.getLong(node, fieldName, true, true));
        }
    }
}
