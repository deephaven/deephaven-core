//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit InstantNanosFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MAX_CONVERTIBLE_MILLIS;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MILLI;

public class InstantNanosFromMillisMaterializer extends LongMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new InstantNanosFromMillisMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new InstantNanosFromMillisMaterializer(dataReader, numValues);
        }
    };

    public static long convertValue(long value) {
        if (value > MAX_CONVERTIBLE_MILLIS || value < -MAX_CONVERTIBLE_MILLIS) {
            throw new UncheckedDeephavenException("Converting " + value + " millis to nanos would overflow");
        }
        return value * MILLI;
    }

    private final PageValueReader dataReader;

    private InstantNanosFromMillisMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private InstantNanosFromMillisMaterializer(PageValueReader dataReader, long nullValue, int numValues) {
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
