//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface PageMaterializer {

    /**
     * Get the internal type used by Deephaven to represent a Parquet
     * {@link LogicalTypeAnnotation.DecimalLogicalTypeAnnotation Decimal} logical type
     */
    static Class<?> resolveDecimalLogicalType(
            final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
        // This pair of values (precision=1, scale=0) is set at write time as a marker so that we can recover
        // the fact that the type is a BigInteger, not a BigDecimal when the fies are read.
        if (decimalLogicalType.getPrecision() == 1 && decimalLogicalType.getScale() == 0) {
            return BigInteger.class;
        }
        return BigDecimal.class;
    }

    void fillNulls(int startIndex, int endIndex);

    void fillValues(int startIndex, int endIndex);

    Object fillAll();

    Object data();
}
