//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.BigDecimalParquetBytesCodec;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.values.ValuesReader;

import java.math.BigDecimal;

public class BigDecimalFromBytesMaterializer extends BigDecimalMaterializerBase implements PageMaterializer {

    public static final class Factory implements PageMaterializerFactory {

        final ObjectCodec<BigDecimal> codec;

        public Factory(int precision, int scale) {
            this.codec = new BigDecimalParquetBytesCodec(precision, scale);
        }

        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BigDecimalFromBytesMaterializer(dataReader, (BigDecimal) nullValue, numValues, codec);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BigDecimalFromBytesMaterializer(dataReader, numValues, codec);
        }
    }

    final ValuesReader dataReader;
    final ObjectCodec<BigDecimal> codec;

    private BigDecimalFromBytesMaterializer(ValuesReader dataReader, int numValues,
            ObjectCodec<BigDecimal> codec) {
        this(dataReader, null, numValues, codec);
    }

    private BigDecimalFromBytesMaterializer(ValuesReader dataReader, BigDecimal nullValue, int numValues,
            ObjectCodec<BigDecimal> codec) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
        this.codec = codec;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            final byte[] bytes = dataReader.readBytes().getBytes();
            data[ii] = codec.decode(bytes, 0, bytes.length);
        }
    }
}
