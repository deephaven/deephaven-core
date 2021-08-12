/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit JsonNodeCharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableDoubleChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonNodeDoubleFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeDoubleFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk, final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset, final int destOffset, final int length
    ) {
        final WritableDoubleChunk<Attributes.Values> output = publisherChunk.asWritableDoubleChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            output.set(ii + destOffset, JsonNodeUtil.getDouble(node, fieldName, true, true));
        }
    }
}
