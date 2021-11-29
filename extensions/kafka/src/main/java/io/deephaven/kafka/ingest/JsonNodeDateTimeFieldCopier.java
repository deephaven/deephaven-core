package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.time.DateTimeUtils;

public class JsonNodeDateTimeFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeDateTimeFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableLongChunk<Values> output = publisherChunk.asWritableLongChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            final long valueAsLong = JsonNodeUtil.getLong(node, fieldName, true, true);
            output.set(ii + destOffset, DateTimeUtils.autoEpochToNanos(valueAsLong));
        }
    }
}
