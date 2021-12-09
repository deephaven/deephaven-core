package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordLongFieldCopierWithMultiplier implements FieldCopier {
    private final String[] fieldPath;
    private final long multiplier;

    public GenericRecordLongFieldCopierWithMultiplier(final String fieldName, final String separator,
            final long multiplier) {
        this.fieldPath = GenericRecordUtil.getFieldPath(fieldName, separator);
        this.multiplier = multiplier;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableLongChunk<Values> output = publisherChunk.asWritableLongChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord record = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Long value = (Long) GenericRecordUtil.getPath(record, fieldPath);
            long unbox = TypeUtils.unbox(value);
            if (unbox != QueryConstants.NULL_LONG) {
                unbox *= multiplier;
            }
            output.set(ii + destOffset, unbox);
        }
    }
}
