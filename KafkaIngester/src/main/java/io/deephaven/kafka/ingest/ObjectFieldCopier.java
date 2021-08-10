package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import org.apache.avro.generic.GenericRecord;

public class ObjectFieldCopier implements GenericRecordFieldCopier {
    private final String fieldName;

    public ObjectFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> publisherChunk) {
        final WritableObjectChunk<Object, Attributes.Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < inputChunk.size(); ++ii) {
            final GenericRecord genericRecord =  (GenericRecord)inputChunk.get(ii);
            final Object value = genericRecord.get(fieldName);
            output.set(ii, value);
        }
    }
}
