//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;

import java.util.Arrays;

public class DoubleMaterializer implements PageMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new DoubleMaterializer(dataReader, (double) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new DoubleMaterializer(dataReader, numValues);
        }
    };

    private final ValuesReader dataReader;

    private final double nullValue;
    private final double[] data;

    private DoubleMaterializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private DoubleMaterializer(ValuesReader dataReader, double nullValue, int numValues) {
        this.dataReader = dataReader;
        this.nullValue = nullValue;
        this.data = new double[numValues];
    }

    @Override
    public void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = dataReader.readDouble();
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
