/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharFieldCopier and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.kafka.ingest;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.util.QueryConstants;
import org.apache.avro.generic.GenericRecord;

public class LongFieldCopier implements GenericRecordFieldCopier {
    private final String fieldName;

    public LongFieldCopier(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public void copyField(ObjectChunk<Object, Attributes.Values> inputChunk, WritableChunk<Attributes.Values> publisherChunk) {
        final WritableLongChunk<Attributes.Values> output = publisherChunk.asWritableLongChunk();
        for (int ii = 0; ii < inputChunk.size(); ++ii) {
            final GenericRecord genericRecord =  (GenericRecord)inputChunk.get(ii);
            final Long value = (Long)genericRecord.get(fieldName);
            output.set(ii, value == null ? QueryConstants.NULL_LONG : value);
        }
    }
}
