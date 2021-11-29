package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.BooleanUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordBooleanFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordBooleanFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableByteChunk<Values> output = publisherChunk.asWritableByteChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Boolean value = genericRecord == null ? null : (Boolean) genericRecord.get(fieldName);
            output.set(ii + destOffset, BooleanUtils.booleanAsByte(value));
        }
    }}
