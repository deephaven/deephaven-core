//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.util.Arrays;

public class CharMaterializer implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new CharMaterializer(dataReader, (char) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new CharMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private final char nullValue;
    private final char[] data;

    private CharMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, (char) 0, numValues);
    }

    private CharMaterializer(ValuesReader dataReader, char nullValue, int numValues) {
        this.dataReader = dataReader;
        this.nullValue = nullValue;
        this.data = new char[numValues];
    }

    @Override
    public void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = (char) dataReader.readInteger();
        }
    }

    @Override
    public Object fillAll() {
        fillValues(0, data.length);
        return data;
    }

    @Override
    public Object data() {
        return data;
    }
}
