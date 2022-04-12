package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Objects;
import java.util.regex.Pattern;

public class GenericRecordStringFieldCopier extends GenericRecordFieldCopier {
    public GenericRecordStringFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema) {
        super(fieldPathStr, separator, schema);
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
            final GenericRecord record = (GenericRecord)inputChunk.get(ii + sourceOffset);
            final Object value = GenericRecordUtil.getPath(record, fieldPath);
            output.set(ii + destOffset, value == null ? null : Objects.toString(value));
        }
    }
}
