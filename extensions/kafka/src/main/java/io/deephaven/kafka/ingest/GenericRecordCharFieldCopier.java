package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.regex.Pattern;

public class GenericRecordCharFieldCopier extends GenericRecordFieldCopier {
    public GenericRecordCharFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema) {
        super(fieldPathStr, separator, schema);
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableCharChunk<Values> output = publisherChunk.asWritableCharChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord record = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Character value = (Character) GenericRecordUtil.getPath(record, fieldPath);
            output.set(ii + destOffset, TypeUtils.unbox(value));
        }
    }
}
