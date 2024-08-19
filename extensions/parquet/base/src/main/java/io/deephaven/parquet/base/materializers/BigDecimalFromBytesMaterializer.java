//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.values.ValuesReader;

import java.math.BigDecimal;

public class BigDecimalFromBytesMaterializer extends ObjectMaterializerBase<BigDecimal> implements PageMaterializer {

    public static final class Factory implements PageMaterializerFactory {

        final ObjectCodec<BigDecimal> codec;

        public Factory(ObjectCodec<BigDecimal> codec) {
            this.codec = codec;
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

    private final ValuesReader dataReader;
    private final ObjectCodec<BigDecimal> codec;

    private BigDecimalFromBytesMaterializer(ValuesReader dataReader, int numValues,
            ObjectCodec<BigDecimal> codec) {
        this(dataReader, null, numValues, codec);
    }

    private BigDecimalFromBytesMaterializer(ValuesReader dataReader, BigDecimal nullValue, int numValues,
            ObjectCodec<BigDecimal> codec) {
        super(nullValue, new BigDecimal[numValues]);
        this.dataReader = dataReader;
        this.codec = codec;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = codec.decode(dataReader.readBytes().toByteBuffer());
        }
    }
}
