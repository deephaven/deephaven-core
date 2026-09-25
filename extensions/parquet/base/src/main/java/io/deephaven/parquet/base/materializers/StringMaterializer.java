//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

public class StringMaterializer extends ObjectMaterializerBase<String> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new StringMaterializer(dataReader, (String) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new StringMaterializer(dataReader, numValues);
        }
    };

    private final PageValueReader dataReader;

    private StringMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    private StringMaterializer(PageValueReader dataReader, String nullValue, int numValues) {
        super(nullValue, new String[numValues]);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = dataReader.readBytes().toStringUsingUTF8();
        }
    }
}
