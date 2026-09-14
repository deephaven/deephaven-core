//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

import java.math.BigDecimal;

public class BigDecimalFromLongMaterializer extends ObjectMaterializerBase<BigDecimal> implements PageMaterializer {

    public static final class Factory implements PageMaterializerFactory {
        final int scale;

        public Factory(final int scale) {
            this.scale = scale;
        }

        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new BigDecimalFromLongMaterializer(dataReader, (BigDecimal) nullValue, numValues, scale);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new BigDecimalFromLongMaterializer(dataReader, numValues, scale);
        }
    };

    private final PageValueReader dataReader;
    private final int scale;

    private BigDecimalFromLongMaterializer(PageValueReader dataReader, int numValues, int scale) {
        this(dataReader, null, numValues, scale);
    }

    private BigDecimalFromLongMaterializer(PageValueReader dataReader, BigDecimal nullValue, int numValues, int scale) {
        super(nullValue, new BigDecimal[numValues]);
        this.dataReader = dataReader;
        this.scale = scale;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = BigDecimal.valueOf(dataReader.readLong(), scale);
        }
    }
}
