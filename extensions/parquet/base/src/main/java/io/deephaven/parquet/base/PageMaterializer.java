package io.deephaven.parquet.base;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Arrays;

interface PageMaterializer {
    interface Factory {
        PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues);

        PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues);
    }

    Factory IntFactory = new Factory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new IntMaterializer(dataReader, (int) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new IntMaterializer(dataReader, numValues);
        }
    };

    Factory LongFactory = new Factory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LongMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LongMaterializer(dataReader, numValues);
        }
    };

    Factory FloatFactory = new Factory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new FloatMaterializer(dataReader, (float) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new FloatMaterializer(dataReader, numValues);
        }
    };

    Factory DoubleFactory = new Factory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new DoubleMaterializer(dataReader, (double) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new DoubleMaterializer(dataReader, numValues);
        }
    };

    Factory BoolFactory = new Factory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BoolMaterializer(dataReader, (byte) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BoolMaterializer(dataReader, numValues);
        }
    };

    Factory BlobFactory = new Factory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BlobMaterializer(dataReader, (Binary) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BlobMaterializer(dataReader, numValues);
        }
    };

    static Factory factoryForType(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
        switch (primitiveTypeName) {
            case INT32:
                return IntFactory;
            case INT64:
                return LongFactory;
            case FLOAT:
                return FloatFactory;
            case DOUBLE:
                return DoubleFactory;
            case BOOLEAN:
                return BoolFactory;
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
            case INT96: {
                return BlobFactory;
            }
            default:
                throw new RuntimeException("Unexpected type name:" + primitiveTypeName);
        }
    }

    void fillNulls(int startIndex, int endIndex);

    void fillValues(int startIndex, int endIndex);

    Object fillAll();

    Object data();

    class IntMaterializer implements PageMaterializer {
        final ValuesReader dataReader;

        final int nullValue;
        final int[] data;

        IntMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        IntMaterializer(ValuesReader dataReader, int nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new int[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readInteger();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class LongMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final long nullValue;
        final long[] data;

        LongMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0, numValues);
        }

        LongMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new long[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readLong();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class FloatMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final float nullValue;
        final float[] data;

        FloatMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0.0f, numValues);
        }

        FloatMaterializer(ValuesReader dataReader, float nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new float[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readFloat();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class DoubleMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final double nullValue;
        final double[] data;

        DoubleMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, 0.0, numValues);
        }

        DoubleMaterializer(ValuesReader dataReader, double nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new double[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readDouble();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class BoolMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final byte nullValue;
        final byte[] data;

        BoolMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, (byte) 0, numValues);
        }

        BoolMaterializer(ValuesReader dataReader, byte nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new byte[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = (byte) (dataReader.readBoolean() ? 1 : 0);
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }

    class BlobMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final Binary nullValue;
        final Binary[] data;

        BlobMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        BlobMaterializer(ValuesReader dataReader, Binary nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new Binary[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readBytes();
            }
        }

        @Override
        public Object fillAll() {
            fillValues(0, data.length);
            return data;
        }

        @Override
        public Object data() {
            return data;
        }
    }
}
