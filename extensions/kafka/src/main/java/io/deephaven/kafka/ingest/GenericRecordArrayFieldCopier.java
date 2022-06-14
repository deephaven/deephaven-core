/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;

import static io.deephaven.util.type.ArrayTypeUtils.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Array;
import java.util.regex.Pattern;

import static io.deephaven.util.QueryConstants.*;

public class GenericRecordArrayFieldCopier extends GenericRecordFieldCopier {
    private final ArrayConverter arrayConverter;
    public GenericRecordArrayFieldCopier(
            final String fieldPathStr,
            final Pattern separator,
            final Schema schema,
            final Class<?> componentType) {
        super(fieldPathStr, separator, schema);
        arrayConverter = ArrayConverter.makeFor(componentType);
    }

    private static <T> T[] convertObjectArray(final GenericArray<?> ga, final T[] emptyArray, final Class<T> componentType) {
        final int gaSize = ga.size();
        if (gaSize == 0) {
            return emptyArray;
        }
        final T[] out = (T[]) Array.newInstance(componentType, ga.size());
        int i = 0;
        for (Object o : ga) {
            out[i] = componentType.cast(o);
            ++i;
        }
        return out;
    }

    private interface ArrayConverter {
        Object convert(final GenericArray<?> genericArray);
        static ArrayConverter makeFor(Class<?> componentType) {
            if (componentType.equals(byte.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return EMPTY_BYTE_ARRAY;
                    }
                    final byte[] out = new byte[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_BYTE : (byte) o;
                        ++i;
                    }
                    return out;
                };
            }
            // There is no "SHORT" in Avro.

            if (componentType.equals(int.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return EMPTY_INT_ARRAY;
                    }
                    final int[] out = new int[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_INT : (int) o;
                        ++i;
                    }
                    return out;
                };
            }
            if (componentType.equals(long.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return EMPTY_LONG_ARRAY;
                    }
                    final long[] out = new long[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_LONG : (long) o;
                        ++i;
                    }
                    return out;
                };
            }
            if (componentType.equals(float.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return EMPTY_FLOAT_ARRAY;
                    }
                    final float[] out = new float[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_FLOAT : (float) o;
                        ++i;
                    }
                    return out;
                };
            }
            if (componentType.equals(double.class)) {
                return (GenericArray<?> ga) -> {
                    final int gaSize = ga.size();
                    if (gaSize == 0) {
                        return EMPTY_DOUBLE_ARRAY;
                    }
                    final double[] out = new double[gaSize];
                    int i = 0;
                    for (Object o : ga) {
                        out[i] = (o == null) ? NULL_DOUBLE : (double) o;
                        ++i;
                    }
                    return out;
                };
            }
            if (componentType.equals(boolean.class)) {
                return (GenericArray<?> ga) -> convertObjectArray(ga, EMPTY_BOOLEANBOXED_ARRAY, boolean.class);
            }
            if (componentType.equals(String.class)) {
                return (GenericArray<?> ga) -> convertObjectArray(ga, EMPTY_STRING_ARRAY, String.class);
            }
            return (GenericArray<?> ga) -> convertObjectArray(ga, EMPTY_OBJECT_ARRAY, Object.class);
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
