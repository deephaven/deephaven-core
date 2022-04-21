package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.vector.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.util.regex.Pattern;

import static io.deephaven.util.QueryConstants.*;

public class GenericRecordVectorFieldCopier extends GenericRecordFieldCopier {
    private final ArrayConverter arrayConverter;
    public GenericRecordVectorFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema, final Class<?> componentType) {
        super(fieldPathStr, separator, schema);
        arrayConverter = ArrayConverter.makeFor(componentType);
    }

    private interface ArrayConverter {
        Vector<?> convert(final GenericArray<?> genericArray);
        static ArrayConverter makeFor(Class<?> componentType) {
            if (componentType.equals(byte.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return ByteVectorDirect.ZERO_LEN_VECTOR;
                    }
                    final byte[] out = new byte[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_BYTE : (byte) o;
                        ++i;
                    }
                    return new ByteVectorDirect(out);
                };
            }
            // There is no "SHORT" in Avro.

            if (componentType.equals(int.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return IntVectorDirect.ZERO_LEN_VECTOR;
                    }
                    final int[] out = new int[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_INT : (int) o;
                        ++i;
                    }
                    return new IntVectorDirect(out);
                };
            }
            if (componentType.equals(long.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return LongVectorDirect.ZERO_LEN_VECTOR;
                    }
                    final long[] out = new long[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_LONG : (long) o;
                        ++i;
                    }
                    return new LongVectorDirect(out);
                };
            }
            if (componentType.equals(float.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return FloatVectorDirect.ZERO_LEN_VECTOR;
                    }
                    final float[] out = new float[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_FLOAT : (float) o;
                        ++i;
                    }
                    return new FloatVectorDirect(out);
                };
            }
            if (componentType.equals(double.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return DoubleVectorDirect.ZERO_LEN_VECTOR;
                    }
                    final double[] out = new double[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_DOUBLE : (double) o;
                        ++i;
                    }
                    return new DoubleVectorDirect(out);
                };
            }
            if (componentType.equals(boolean.class) || componentType.equals(String.class)) {
                return (GenericArray<?> ga) -> {
                    final Object[] out = new Object[ga.size()];
                    ga.toArray(out);
                    return new ObjectVectorDirect<>(out);
                };
            }
            throw new IllegalStateException("Unsupported component type " + componentType);
        }
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
            final GenericArray<?> genericArray = (GenericArray<?>) GenericRecordUtil.getPath(record, fieldPath);
            if (genericArray == null) {
                output.set(ii + destOffset, null);
                continue;
            }
            output.set(ii + destOffset, arrayConverter.convert(genericArray));
        }
    }
}
