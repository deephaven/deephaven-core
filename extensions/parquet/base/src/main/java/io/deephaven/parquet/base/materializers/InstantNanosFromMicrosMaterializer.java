//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MAX_CONVERTIBLE_MICROS;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MICRO;

public class InstantNanosFromMicrosMaterializer extends LongMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new InstantNanosFromMicrosMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new InstantNanosFromMicrosMaterializer(dataReader, numValues);
        }
    };

    public static long convertValue(long value) {
        if (value > MAX_CONVERTIBLE_MICROS || value < -MAX_CONVERTIBLE_MICROS) {
            throw new UncheckedDeephavenException("Converting " + value + " micros to nanos would overflow");
        }
        return value * MICRO;
    }

    private final ValuesReader dataReader;

    private InstantNanosFromMicrosMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private InstantNanosFromMicrosMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
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
