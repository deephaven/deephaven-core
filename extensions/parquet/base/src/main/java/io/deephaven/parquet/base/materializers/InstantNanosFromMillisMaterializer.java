//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit InstantNanosFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

public class InstantNanosFromMillisMaterializer extends LongMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new InstantNanosFromMillisMaterializer(dataReader, (long) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new InstantNanosFromMillisMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private InstantNanosFromMillisMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private InstantNanosFromMillisMaterializer(ValuesReader dataReader, long nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = DateTimeUtils.millisToNanos(dataReader.readLong());
        }
    }
}
