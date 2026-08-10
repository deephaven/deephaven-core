//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.math.BigInteger;

/**
 * Materializes an unsigned 64-bit parquet int as a {@link BigInteger}, since such values do not fit in any Java
 * primitive.
 */
public class BigIntegerFromUnsignedLongMaterializer extends ObjectMaterializerBase<BigInteger>
        implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BigIntegerFromUnsignedLongMaterializer(dataReader, (BigInteger) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BigIntegerFromUnsignedLongMaterializer(dataReader, numValues);
        }
    };

    private static final BigInteger TWO_TO_THE_64 = BigInteger.ONE.shiftLeft(64);

    public static BigInteger convertValue(long value) {
        final BigInteger signed = BigInteger.valueOf(value);
        return value >= 0 ? signed : signed.add(TWO_TO_THE_64);
    }

    private final ValuesReader dataReader;

    private BigIntegerFromUnsignedLongMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private BigIntegerFromUnsignedLongMaterializer(ValuesReader dataReader, BigInteger nullValue, int numValues) {
        super(nullValue, new BigInteger[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readLong());
        }
    }
}
