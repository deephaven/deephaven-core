package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableByteChunk;
import io.deephaven.util.BooleanUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordBooleanFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordBooleanFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableByteChunk<Attributes.Values> output = publisherChunk.asWritableByteChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Boolean value = genericRecord == null ? null : (Boolean) genericRecord.get(fieldName);
            output.set(ii + destOffset, BooleanUtils.booleanAsByte(value));
        }
    }}
