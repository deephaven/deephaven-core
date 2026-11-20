//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.math.BigDecimal;

/**
 * Materializes an unsigned 64-bit parquet int as a {@link BigDecimal} with a scale of zero, since such values do not
 * fit in any Java primitive.
 */
public class BigDecimalFromUnsignedLongMaterializer extends ObjectMaterializerBase<BigDecimal>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BigDecimalFromUnsignedLongMaterializer(dataReader, (BigDecimal) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BigDecimalFromUnsignedLongMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private BigDecimalFromUnsignedLongMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private BigDecimalFromUnsignedLongMaterializer(ValuesReader dataReader, BigDecimal nullValue, int numValues) {
        super(nullValue, new BigDecimal[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = new BigDecimal(BigIntegerFromUnsignedLongMaterializer.convertValue(dataReader.readLong()));
        }
    }
}
