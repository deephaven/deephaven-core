//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.parquet.base.materializers.*;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface PageMaterializer {

    /**
     * Get the internal type used by Deephaven to represent a Parquet
     * {@link LogicalTypeAnnotation.DecimalLogicalTypeAnnotation Decimal} logical type
     */
    // TODO Better name for this?
    static Class<?> getDHTypeForDecimalType(
            final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
        // This pair of values (precision=1, scale=0) is set at write time as a marker so that we can recover
        // the fact that the type is a BigInteger, not a BigDecimal when the fies are read.
        if (decimalLogicalType.getPrecision() == 1 && decimalLogicalType.getScale() == 0) {
            return BigInteger.class;
        }
        return BigDecimal.class;
    }

    static PageMaterializerFactory factoryForType(@NotNull final PrimitiveType primitiveType) {
        final PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();
        final LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
        switch (primitiveTypeName) {
            case INT32:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType =
                            (LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalTypeAnnotation;
                    if (intLogicalType.isSigned()) {
                        switch (intLogicalType.getBitWidth()) {
                            case 8:
                                return ByteMaterializer.Factory;
                            case 16:
                                return ShortMaterializer.Factory;
                            case 32:
                                return IntMaterializer.Factory;
                        }
                    } else {
                        switch (intLogicalType.getBitWidth()) {
                            case 8:
                            case 16:
                                return CharMaterializer.Factory;
                            case 32:
                                return LongFromUnsignedIntMaterializer.Factory;
                        }
                    }
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                    return LocalDateMaterializer.Factory;
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType =
                            (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation;
                    if (timeLogicalType.getUnit() != LogicalTypeAnnotation.TimeUnit.MILLIS) {
                        throw new IllegalArgumentException(
                                "Expected unit type to be MILLIS, found " + timeLogicalType.getUnit());
                    }
                    // isAdjustedToUTC parameter is ignored while reading LocalTime from Parquet files
                    return LocalTimeFromMillisMaterializer.Factory;
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
                    return new BigDecimalFromIntMaterializer.Factory(decimalLogicalType.getScale());
                }
                return IntMaterializer.Factory;
            case INT64:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType =
                            (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalTypeAnnotation;
                    if (timestampLogicalType.isAdjustedToUTC()) {
                        // The column will store nanoseconds elapsed since epoch as long values
                        switch (timestampLogicalType.getUnit()) {
                            case MILLIS:
                                return InstantNanosFromMillisMaterializer.Factory;
                            case MICROS:
                                return InstantNanosFromMicrosMaterializer.Factory;
                            case NANOS:
                                return LongMaterializer.Factory;
                        }
                    } else {
                        // The column will be stored as LocalDateTime values
                        // Ref:https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#local-semantics-timestamps-not-normalized-to-utc
                        switch (timestampLogicalType.getUnit()) {
                            case MILLIS:
                                return LocalDateTimeFromMillisMaterializer.Factory;
                            case MICROS:
                                return LocalDateTimeFromMicrosMaterializer.Factory;
                            case NANOS:
                                return LocalDateTimeFromNanosMaterializer.Factory;
                        }
                    }
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType =
                            (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalTypeAnnotation;
                    // isAdjustedToUTC parameter is ignored while reading LocalTime from Parquet files
                    switch (timeLogicalType.getUnit()) {
                        case MICROS:
                            return LocalTimeFromMicrosMaterializer.Factory;
                        case NANOS:
                            return LocalTimeFromNanosMaterializer.Factory;
                        default:
                            throw new IllegalArgumentException("Unsupported unit=" + timeLogicalType.getUnit());
                    }
                } else if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
                    return new BigDecimalFromLongMaterializer.Factory(decimalLogicalType.getScale());
                }
                return LongMaterializer.Factory;
            case INT96:
                return InstantFromInt96Materializer.Factory;
            case FLOAT:
                return FloatMaterializer.Factory;
            case DOUBLE:
                return DoubleMaterializer.Factory;
            case BOOLEAN:
                return BoolMaterializer.Factory;
            case BINARY:
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                    return StringMaterializer.Factory;
                }
            case FIXED_LEN_BYTE_ARRAY: // fall through
                if (logicalTypeAnnotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                    final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType =
                            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalTypeAnnotation;
                    if (getDHTypeForDecimalType(decimalLogicalType) == BigInteger.class) {
                        return BigIntegerMaterializer.Factory;
                    }
                    return new BigDecimalFromBytesMaterializer.Factory(
                            decimalLogicalType.getPrecision(), decimalLogicalType.getScale());
                }
                return BlobMaterializer.Factory;
            default:
                throw new RuntimeException("Unexpected type name:" + primitiveTypeName);
        }
    }

    void fillNulls(int startIndex, int endIndex);

    void fillValues(int startIndex, int endIndex);

    Object fillAll();

    Object data();
}
