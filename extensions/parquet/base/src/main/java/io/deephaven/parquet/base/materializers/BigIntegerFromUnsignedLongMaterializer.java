//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

import java.math.BigInteger;

/**
 * Materializes an unsigned 64-bit parquet int as a {@link BigInteger}, since such values do not fit in any Java
 * primitive.
 */
public class BigIntegerFromUnsignedLongMaterializer extends ObjectMaterializerBase<BigInteger>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new BigIntegerFromUnsignedLongMaterializer(dataReader, (BigInteger) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new BigIntegerFromUnsignedLongMaterializer(dataReader, numValues);
        }
    };

    private final PageValueReader dataReader;

    private BigIntegerFromUnsignedLongMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private BigIntegerFromUnsignedLongMaterializer(PageValueReader dataReader, BigInteger nullValue, int numValues) {
        super(nullValue, new BigInteger[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = PageValueConversions.bigIntegerFromUnsignedLong(dataReader.readLong());
        }
    }
}
