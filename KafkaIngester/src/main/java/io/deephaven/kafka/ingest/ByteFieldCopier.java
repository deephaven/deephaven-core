/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableByteChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import org.apache.avro.generic.GenericRecord;

public class ByteFieldCopier implements GenericRecordFieldCopier {
    private final String fieldName;

    public ByteFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> publisherChunk, int sourceOffset, int destOffset, int length) {
        final WritableByteChunk<Attributes.Values> output = publisherChunk.asWritableByteChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord =  (GenericRecord)inputChunk.get(ii + sourceOffset);
            final Byte value = genericRecord == null ? null : (Byte) genericRecord.get(fieldName);
            output.set(ii + destOffset, value == null ? QueryConstants.NULL_BYTE : value);
        }
    }
}
