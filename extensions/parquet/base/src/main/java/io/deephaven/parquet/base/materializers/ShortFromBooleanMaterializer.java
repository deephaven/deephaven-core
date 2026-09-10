//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ShortMaterializer and run "./gradlew replicatePageMaterializers" to regenerate
//
// @formatter:off
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import io.deephaven.parquet.base.PageValueReader;

public class ShortFromBooleanMaterializer extends ShortMaterializerBase implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(PageValueReader dataReader, Object nullValue, int numValues) {
            return new ShortFromBooleanMaterializer(dataReader, (short) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(PageValueReader dataReader, int numValues) {
            return new ShortFromBooleanMaterializer(dataReader, numValues);
        }
    };

    public static short convertValue(boolean value) {
        return (short) (value ? 1 : 0);
    }

    private final PageValueReader dataReader;

    private ShortFromBooleanMaterializer(PageValueReader dataReader, int numValues) {
        this(dataReader, (short) 0, numValues);
    }

    private ShortFromBooleanMaterializer(PageValueReader dataReader, short nullValue, int numValues) {
        super(nullValue, numValues);
        this.dataReader = dataReader;
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readBoolean());
        }
    }
}
