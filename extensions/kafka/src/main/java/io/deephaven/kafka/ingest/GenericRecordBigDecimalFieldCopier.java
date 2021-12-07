package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;

public class GenericRecordBigDecimalFieldCopier implements FieldCopier {
    private final String fieldName;
    private final int scale;

    public GenericRecordBigDecimalFieldCopier(final String fieldName, final int precisionUnused, final int scale) {
        this.fieldName = fieldName;
        this.scale = scale;
    }

    @Override
    public void copyField(
            final ObjectChunk<Object, Values> inputChunk,
            final WritableChunk<Values> publisherChunk,
            final int sourceOffset,
            final int destOffset,
            final int length) {
        final WritableObjectChunk<Object, Values> output = publisherChunk.asWritableObjectChunk();
        for (int ii = 0; ii < length; ++ii) {
            final GenericRecord genericRecord = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final byte[] bytes = genericRecord == null ? null : (byte[]) genericRecord.get(fieldName);
            if (bytes == null) {
                output.set(ii + destOffset, null);
                return;
            }
            final BigInteger bi = new BigInteger(bytes);
            final BigDecimal bd = new BigDecimal(bi, scale);
            output.set(ii + destOffset, bd);
        }
    }
}
