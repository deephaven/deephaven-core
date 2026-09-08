//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

/**
 * Materializes an unsigned 64-bit parquet int as a primitive {@code long}, rejecting values that exceed
 * {@link Long#MAX_VALUE} rather than silently reinterpreting them as negative.
 */
public class LongFromUnsignedLongMaterializer extends LongMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LongFromUnsignedLongMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LongFromUnsignedLongMaterializer(dataReader, numValues);
        }
    };

    /**
     * An unsigned value exceeds {@link Long#MAX_VALUE} exactly when its signed bit pattern is negative.
     */
    public static long convertValue(long value) {
        if (value < 0) {
            throw new UncheckedDeephavenException("Unsigned long value " + Long.toUnsignedString(value)
                    + " is too large to be represented as a long");
        }
        return value;
    }

    private final ValuesReader dataReader;

    private LongFromUnsignedLongMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private LongFromUnsignedLongMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readLong());
        }
    }
}
