//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalTime;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MICRO;

public class LocalTimeFromMicrosMaterializer extends ObjectMaterializerBase<LocalTime> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalTimeFromMicrosMaterializer(dataReader, (LocalTime) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalTimeFromMicrosMaterializer(dataReader, numValues);
        }
    };

    public static LocalTime convertValue(long value) {
        return LocalTime.ofNanoOfDay(value * MICRO);
    }

    private final ValuesReader dataReader;

    private LocalTimeFromMicrosMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalTimeFromMicrosMaterializer(ValuesReader dataReader, LocalTime nullValue, int numValues) {
        super(nullValue, new LocalTime[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readLong());
        }
    }
}
