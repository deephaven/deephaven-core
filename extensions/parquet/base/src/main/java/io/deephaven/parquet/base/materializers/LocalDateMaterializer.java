//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.column.values.ValuesReader;

import java.time.LocalDate;

public class LocalDateMaterializer extends ObjectMaterializerBase<LocalDate> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new LocalDateMaterializer(dataReader, (LocalDate) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new LocalDateMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private LocalDateMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private LocalDateMaterializer(ValuesReader dataReader, LocalDate nullValue, int numValues) {
        super(nullValue, new LocalDate[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = DateTimeUtils.epochDaysAsIntToLocalDate(dataReader.readInteger());
        }
    }
}
