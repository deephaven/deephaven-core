/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit GenericRecordCharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.engine.structures.chunk.Attributes;
import io.deephaven.engine.structures.chunk.ObjectChunk;
import io.deephaven.engine.structures.chunk.WritableIntChunk;
import io.deephaven.engine.structures.chunk.WritableChunk;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.generic.GenericRecord;

public class GenericRecordIntFieldCopier implements FieldCopier {
    private final String fieldName;

    public GenericRecordIntFieldCopier(final String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Attributes.Values> inputChunk,
            final WritableChunk<Attributes.Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableIntChunk<Attributes.Values> output = publisherChunk.asWritableIntChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final Integer value = genericRecord == null ? null : (Integer) genericRecord.get(fieldName);
            output.set(ii + destOffset, TypeUtils.unbox(value));
        }
    }
}
