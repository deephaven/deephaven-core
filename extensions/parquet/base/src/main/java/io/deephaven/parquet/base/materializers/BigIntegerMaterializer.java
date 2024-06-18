//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.BigIntegerParquetBytesCodec;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.values.ValuesReader;

import java.math.BigInteger;
import java.util.Arrays;

public class BigIntegerMaterializer implements PageMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {

        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BigIntegerMaterializer(dataReader, (BigInteger) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BigIntegerMaterializer(dataReader, numValues);
        }
    };

    private static final ObjectCodec<BigInteger> CODEC = new BigIntegerParquetBytesCodec();

    final ValuesReader dataReader;

    final BigInteger nullValue;
    final BigInteger[] data;

    private BigIntegerMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private BigIntegerMaterializer(ValuesReader dataReader, BigInteger nullValue, int numValues) {
        this.dataReader = dataReader;
        this.nullValue = nullValue;
        this.data = new BigInteger[numValues];
    }

    @Override
    public void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            final byte[] bytes = dataReader.readBytes().getBytes();
            data[ii] = CODEC.decode(bytes, 0, bytes.length);
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
