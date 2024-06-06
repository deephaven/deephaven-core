//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit LocalTimeFromMicrosMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalTime;

public class LocalTimeFromNanosMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromNanosPageMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromNanosPageMaterializer(dataReader, numValues);
        }
    };

    private static final class LocalTimeFromNanosPageMaterializer extends LocalTimePageMaterializerBase
            implements PageMaterializer {

        final ValuesReader dataReader;

        private LocalTimeFromNanosPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        private LocalTimeFromNanosPageMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
            super(nullValue, numValues);
            this.dataReader = dataReader;
        }

        @Override
        LocalTime readNext() {
            return DateTimeUtils.nanosOfDayToLocalTime(dataReader.readLong());
        }
    }
}
