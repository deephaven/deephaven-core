/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit GenericRecordCharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordLongFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordLongFieldCopier(final String fieldName) {
        this.fieldName = fieldName;
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
            output.set(ii + destOffset, TypeUtils.unbox(value));
        }
    }
}
