//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit IntMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

public class IntFromBooleanMaterializer extends IntMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new IntFromBooleanMaterializer(dataReader, (int) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new IntFromBooleanMaterializer(dataReader, numValues);
        }
    };

    private final PageValueReader dataReader;

    private IntFromBooleanMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private IntFromBooleanMaterializer(PageValueReader dataReader, int nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = PageValueConversions.intFromBoolean(dataReader.readBoolean());
        }
    }
}
