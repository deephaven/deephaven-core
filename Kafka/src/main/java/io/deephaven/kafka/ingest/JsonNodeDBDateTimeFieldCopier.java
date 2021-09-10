package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.tables.utils.DBTimeUtils;

public class JsonNodeDBDateTimeFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeDBDateTimeFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableObjectChunk<Object, Attributes.Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            final long valueAsLong = JsonNodeUtil.getLong(node, fieldName, true, true);
            output.set(ii + destOffset, DBTimeUtils.autoEpochToTime(valueAsLong));
        }
    }
}
