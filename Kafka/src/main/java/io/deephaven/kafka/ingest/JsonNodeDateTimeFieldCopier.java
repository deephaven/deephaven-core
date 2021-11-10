package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.engine.chunk.*;
import io.deephaven.engine.tables.utils.DateTimeUtils;

public class JsonNodeDateTimeFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeDateTimeFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableLongChunk<Attributes.Values> output = publisherChunk.asWritableLongChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            final long valueAsLong = JsonNodeUtil.getLong(node, fieldName, true, true);
            output.set(ii + destOffset, DateTimeUtils.autoEpochToNanos(valueAsLong));
        }
    }
}
