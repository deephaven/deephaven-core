/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit GenericRecordCharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableDoubleChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordDoubleFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordDoubleFieldCopier(final String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableDoubleChunk<Attributes.Values> output = publisherChunk.asWritableDoubleChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Double value = genericRecord == null ? null : (Double) genericRecord.get(fieldName);
            output.set(ii + destOffset, TypeUtils.unbox(value));
        }
    }
}
