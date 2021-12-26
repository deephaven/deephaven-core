package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.regex.Pattern;

public class GenericRecordBigDecimalFieldCopier extends GenericRecordFieldCopier {
    private final int scale;

    public GenericRecordBigDecimalFieldCopier(
            final String fieldPathStr,
            final Pattern separator,
            final Schema schema,
            final int precisionUnused,
            final int scale) {
        super(fieldPathStr, separator, schema);
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
            final GenericRecord record = (GenericRecord) inputChunk.get(ii + sourceOffset);
            final byte[] bytes = (byte[]) GenericRecordUtil.getPath(record, fieldPath);
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
