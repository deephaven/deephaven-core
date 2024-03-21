//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.column.values.ValuesReader;

interface PageMaterializerFactory {
    PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues);

    PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues);
}
