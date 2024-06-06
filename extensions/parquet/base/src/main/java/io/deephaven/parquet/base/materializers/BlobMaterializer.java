//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

import java.util.Arrays;

public class BlobMaterializer {

    public static final PageMaterializerFactory Factory = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new BlobPageMaterializer(dataReader, (Binary) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new BlobPageMaterializer(dataReader, numValues);
        }
    };

    private static final class BlobPageMaterializer implements PageMaterializer {

        final ValuesReader dataReader;

        final Binary nullValue;
        final Binary[] data;

        private BlobPageMaterializer(ValuesReader dataReader, int numValues) {
            this(dataReader, null, numValues);
        }

        private BlobPageMaterializer(ValuesReader dataReader, Binary nullValue, int numValues) {
            this.dataReader = dataReader;
            this.nullValue = nullValue;
            this.data = new Binary[numValues];
        }

        @Override
        public void fillNulls(int startIndex, int endIndex) {
            Arrays.fill(data, startIndex, endIndex, nullValue);
        }

        @Override
        public void fillValues(int startIndex, int endIndex) {
            for (int ii = startIndex; ii < endIndex; ii++) {
                data[ii] = dataReader.readBytes();
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
}
