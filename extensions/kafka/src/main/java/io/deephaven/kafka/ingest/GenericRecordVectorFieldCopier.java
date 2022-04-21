package io.deephaven.kafka.ingest;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.vector.ObjectVectorDirect;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

import java.lang.reflect.Array;
import java.util.regex.Pattern;

public class GenericRecordVectorFieldCopier extends GenericRecordFieldCopier {
    private final Class<?> componentType;
    private final ArrayConverter arrayConverter;
    public GenericRecordVectorFieldCopier(final String fieldPathStr, final Pattern separator, final Schema schema, final Class<?> componentType) {
        super(fieldPathStr, separator, schema);
        this.componentType = componentType;
        arrayConverter = ArrayConverter.makeFor(componentType);
    }

    private interface ArrayConverter {
        ObjectVectorDirect<?> convert(Object array);
        static ArrayConverter makeFor(Class<?> componentType) {
            if (componentType.equals(byte.class)) {
                return arr -> new ObjectVectorDirect<>((byte[]) arr);
            }
            if (componentType.equals(int.class)) {
                return arr -> new ObjectVectorDirect<>((int[]) arr);
            }
            if (componentType.equals(long.class)) {
                return arr -> new ObjectVectorDirect<>((long[]) arr);
            }
            if (componentType.equals(float.class)) {
                return arr -> new ObjectVectorDirect<>((float[]) arr);
            }
            if (componentType.equals(double.class)) {
                return arr -> new ObjectVectorDirect<>((double[]) arr);
            }
            if (componentType.equals(boolean.class) || componentType.equals(String.class)) {
                return arr -> new ObjectVectorDirect<>((Object[]) arr);
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
                return;
            }
            final int arrSize = genericArray.size();
            if (arrSize == 0) {
                output.set(ii + destOffset, ObjectVectorDirect.ZERO_LEN_VECTOR);
                return;
            }
            final Object resultArr = Array.newInstance(componentType, arrSize);
            output.set(ii + destOffset, arrayConverter.convert(resultArr));
        }
    }
}
