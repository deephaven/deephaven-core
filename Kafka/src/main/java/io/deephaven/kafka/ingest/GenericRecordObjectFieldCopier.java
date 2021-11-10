/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.chunk.WritableChunk;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordObjectFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordObjectFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableObjectChunk<Object, Attributes.Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Object value = genericRecord == null ? null : genericRecord.get(fieldName);
            output.set(ii + destOffset, value);
        }
    }
}
