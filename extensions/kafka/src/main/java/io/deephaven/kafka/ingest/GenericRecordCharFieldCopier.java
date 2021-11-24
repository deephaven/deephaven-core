package io.deephaven.kafka.ingest;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableCharChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordCharFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordCharFieldCopier(final String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableCharChunk<Attributes.Values> output = publisherChunk.asWritableCharChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Character value = genericRecord == null ? null : (Character) genericRecord.get(fieldName);
            output.set(ii + destOffset, TypeUtils.unbox(value));
        }
    }
}
