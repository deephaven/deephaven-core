package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
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
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableLongChunk<Attributes.Values> output = publisherChunk.asWritableLongChunk();
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
