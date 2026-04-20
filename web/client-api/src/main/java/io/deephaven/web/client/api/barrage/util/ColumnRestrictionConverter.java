//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import com.google.protobuf.Any;
import io.deephaven.web.client.api.ColumnRestriction;

/**
 * Functional interface for converting parsed restriction data into a ColumnRestriction object.
 */
@FunctionalInterface
public interface ColumnRestrictionConverter {
    /**
     * Convert parsed restriction data into a ColumnRestriction object.
     *
     * @param restrictionData The parsed restriction data from protobuf
     * @return A ColumnRestriction object, or null if conversion fails
     */
    ColumnRestriction convert(Any restrictionData);
}

