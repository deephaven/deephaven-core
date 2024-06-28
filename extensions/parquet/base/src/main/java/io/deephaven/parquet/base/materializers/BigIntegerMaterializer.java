//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit BigDecimalFromBytesMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.util.codec.ObjectCodec;
import org.apache.parquet.column.values.ValuesReader;

import java.math.BigInteger;

public class BigIntegerMaterializer extends ObjectMaterializerBase<BigInteger> implements PageMaterializer {

    public static final class Factory implements PageMaterializerFactory {

        final ObjectCodec<BigInteger> codec;

        public Factory(ObjectCodec<BigInteger> codec) {
            this.codec = codec;
        }

        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BigIntegerMaterializer(dataReader, (BigInteger) nullValue, numValues, codec);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BigIntegerMaterializer(dataReader, numValues, codec);
        }
    }

    private final ValuesReader dataReader;
    private final ObjectCodec<BigInteger> codec;

    private BigIntegerMaterializer(ValuesReader dataReader, int numValues,
            ObjectCodec<BigInteger> codec) {
        this(dataReader, null, numValues, codec);
    }

    private BigIntegerMaterializer(ValuesReader dataReader, BigInteger nullValue, int numValues,
            ObjectCodec<BigInteger> codec) {
        super(nullValue, new BigInteger[numValues]);
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
