package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordLongFieldCopierWithMultiplier implements FieldCopier {
    private final String fieldName;
    private final long multiplier;

    public GenericRecordLongFieldCopierWithMultiplier(final String fieldName, final long multiplier) {
        this.fieldName = fieldName;
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
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Long value = genericRecord == null ? null : (Long) genericRecord.get(fieldName);
            long unbox = TypeUtils.unbox(value);
            if (unbox != QueryConstants.NULL_LONG) {
                unbox *= multiplier;
            }
            output.set(ii + destOffset, unbox);
        }
    }
}
