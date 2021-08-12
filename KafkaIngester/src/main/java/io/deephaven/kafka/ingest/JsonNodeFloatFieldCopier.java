/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit JsonNodeCharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableFloatChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import com.fasterxml.jackson.databind.JsonNode;

public class JsonNodeFloatFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeFloatFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk, final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset, final int destOffset, final int length
    ) {
        final WritableFloatChunk<Attributes.Values> output = publisherChunk.asWritableFloatChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            output.set(ii + destOffset, JsonNodeUtil.getFloat(node, fieldName, true, true));
        }
    }
}
