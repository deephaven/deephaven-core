//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

public class StringMaterializer extends ObjectMaterializerBase<String> implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PlainBinaryPageReaderFactory();

    private final ValuesReader dataReader;

    StringMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, null, numValues);
    }

    StringMaterializer(ValuesReader dataReader, String nullValue, int numValues) {
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
