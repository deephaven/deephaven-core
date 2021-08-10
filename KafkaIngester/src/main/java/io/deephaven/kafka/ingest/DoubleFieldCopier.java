/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableDoubleChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import org.apache.avro.generic.GenericRecord;

public class DoubleFieldCopier implements GenericRecordFieldCopier {
    private final String fieldName;

    public DoubleFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> publisherChunk) {
        final WritableDoubleChunk<Attributes.Values> output = publisherChunk.asWritableDoubleChunk();
        for (int ii = 0; ii < inputChunk.size(); ++ii) {
            final GenericRecord genericRecord =  (GenericRecord)inputChunk.get(ii);
            final Double value = (Double)genericRecord.get(fieldName);
            output.set(ii, value == null ? QueryConstants.NULL_DOUBLE : value);
        }
    }
}
