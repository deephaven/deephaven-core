/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableShortChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.type.TypeUtils;
import org.apache.avro.generic.GenericRecord;

public class ShortFieldCopier implements GenericRecordFieldCopier {
    private final String fieldName;

    public ShortFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> publisherChunk, int sourceOffset, int destOffset, int length) {
        final WritableShortChunk<Attributes.Values> output = publisherChunk.asWritableShortChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord)inputChunk.get(ii + sourceOffset);
            final Short value = genericRecord == null ? null : (Short) genericRecord.get(fieldName);
            output.set(ii + destOffset, TypeUtils.unbox(value));
        }
    }
}
