//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

public interface PageMaterializer {

    void fillNulls(int startIndex, int endIndex);

    void fillValues(int startIndex, int endIndex);

    Object fillAll();

    Object data();
}
