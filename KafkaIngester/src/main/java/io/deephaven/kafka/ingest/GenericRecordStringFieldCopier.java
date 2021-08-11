package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import org.apache.avro.generic.GenericRecord;

import java.util.Objects;

public class GenericRecordStringFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordStringFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> publisherChunk, int sourceOffset, int destOffset, int length) {
        final WritableObjectChunk<Object, Attributes.Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord)inputChunk.get(ii + sourceOffset);
            final Object value = genericRecord == null ? null : genericRecord.get(fieldName);
            output.set(ii + destOffset, value == null ? null : Objects.toString(value));
        }
    }
}
