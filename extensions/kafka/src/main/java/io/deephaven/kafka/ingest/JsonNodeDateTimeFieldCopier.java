/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;

public class JsonNodeDateTimeFieldCopier implements FieldCopier {
    private final JsonPointer fieldPointer;

    public JsonNodeDateTimeFieldCopier(final String fieldPointerStr) {
        this.fieldPointer = JsonPointer.compile(fieldPointerStr);
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
            final DateTime dateTime = JsonNodeUtil.getDateTime(node, fieldPointer, true, true);
            output.set(ii + destOffset, DateTimeUtils.nanos(dateTime));
        }
    }
}
