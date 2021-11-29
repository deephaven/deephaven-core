package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.BooleanUtils;

public class JsonNodeBooleanFieldCopier implements FieldCopier {
    private final String fieldName;

    public JsonNodeBooleanFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values> publisherChunk, int sourceOffset, int destOffset, int length) {
        final WritableByteChunk<Values> output = publisherChunk.asWritableByteChunk();
        for (int ii = 0; ii < length; ++ii) {
            final JsonNode node = (JsonNode) inputChunk.get(ii + sourceOffset);
            final String valueAsString = JsonNodeUtil.getString(node, fieldName, true, true);
            final Boolean valueAsBoolean;
            if (valueAsString == null) {
                valueAsBoolean = null;
            } else {
                switch(valueAsString.trim()) {
                    case "TRUE":
                    case "True":
                    case "true":
                    case "T":
                    case "t":
                    case "1":
                        valueAsBoolean = Boolean.TRUE;
                        break;
                    case "FALSE":
                    case "False":
                    case "false":
                    case "F":
                    case "f":
                        valueAsBoolean = Boolean.FALSE;
                        break;
                    case "":
                        valueAsBoolean = null;
                        break;
                    default:
                        throw new UncheckedDeephavenException("value " + valueAsString + " not recognized as Boolean for field " + fieldName);
                }
            }
            output.set(ii + destOffset, BooleanUtils.booleanAsByte(valueAsBoolean));
        }
    }
}
