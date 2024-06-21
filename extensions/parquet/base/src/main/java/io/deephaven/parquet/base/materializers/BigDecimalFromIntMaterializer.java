//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit BigDecimalFromLongMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.math.BigDecimal;

public class BigDecimalFromIntMaterializer extends ObjectMaterializerBase<BigDecimal> implements PageMaterializer {

    public static final class Factory implements PageMaterializerFactory {
        final int scale;

        public Factory(final int scale) {
            this.scale = scale;
        }

        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BigDecimalFromIntMaterializer(dataReader, (BigDecimal) nullValue, numValues, scale);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BigDecimalFromIntMaterializer(dataReader, numValues, scale);
        }
    };

    private final ValuesReader dataReader;
    private final int scale;

    private BigDecimalFromIntMaterializer(ValuesReader dataReader, int numValues, int scale) {
        this(dataReader, null, numValues, scale);
    }

    private BigDecimalFromIntMaterializer(ValuesReader dataReader, BigDecimal nullValue, int numValues, int scale) {
        super(nullValue, new BigDecimal[numValues]);
        this.dataReader = dataReader;
        this.scale = scale;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = BigDecimal.valueOf(dataReader.readInteger(), scale);
        }
    }
}
