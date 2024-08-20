//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.core.JsonPointer;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.chunk.attributes.Values;

public class JsonNodeCharFieldCopier implements FieldCopier {
    private final JsonPointer fieldPointer;

    public JsonNodeCharFieldCopier(final String fieldPointerStr) {
        this.fieldPointer = JsonPointer.compile(fieldPointerStr);
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk, final WritableChunk<Values> publisherChunk,
            final int sourceOffset, final int destOffset, final int length) {
        final WritableCharChunk<Values> output = publisherChunk.asWritableCharChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            output.set(ii + destOffset, JsonNodeUtil.getChar(node, fieldPointer, true, true));
        }
    }
}
