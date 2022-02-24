package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
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
            final Object bytesObj = GenericRecordUtil.getPath(record, fieldPath);
            if (bytesObj == null) {
                output.set(ii + destOffset, null);
                return;
            }
            if (bytesObj instanceof byte[]) {
                final byte[] bytes = (byte[]) bytesObj;
                final BigInteger bi = new BigInteger(bytes);
                final BigDecimal bd = new BigDecimal(bi, scale);
                output.set(ii + destOffset, bd);
            } else if (bytesObj instanceof ByteBuffer) {
                final ByteBuffer bb = (ByteBuffer) bytesObj;
                final BigInteger bi;
                if (bb.hasArray()) {
                    bi = new BigInteger(bb.array(), bb.position() + bb.arrayOffset(), bb.remaining());
                } else {
                    final byte[] bytes = new byte[bb.remaining()];
                    bb.get(bytes);
                    bi = new BigInteger(bytes);
                }
                final BigDecimal bd = new BigDecimal(bi, scale);
                output.set(ii + destOffset, bd);
            } else {
                throw new IllegalStateException("Object of type " + bytesObj.getClass().getName() + " not recognized for decimal type backing");
            }
        }
    }
}
