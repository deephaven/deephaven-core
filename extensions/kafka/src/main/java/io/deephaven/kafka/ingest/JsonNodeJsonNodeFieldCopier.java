//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.core.JsonPointer;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.chunk.attributes.Values;

public class JsonNodeJsonNodeFieldCopier implements FieldCopier {
    private final JsonPointer fieldPointer;

    public JsonNodeJsonNodeFieldCopier(final String fieldPointerStr) {
        this.fieldPointer = JsonPointer.compile(fieldPointerStr);
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableObjectChunk<Object, Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode baseNode = (JsonNode) inputChunk.get(ii + sourceOffset);
            final JsonNode node = (baseNode == null) ? null : baseNode.at(fieldPointer);
            output.set(ii + destOffset, node);
        }
    }
}
