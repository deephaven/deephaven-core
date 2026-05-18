//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsNullable;
import jsinterop.base.JsPropertyMap;

/**
 * Client-side validator for a column restriction. Given a proposed value and the restriction's data object, returns a
 * human-readable error message if the value is invalid, or {@code null} if it is valid.
 *
 * <p>
 * Validators are registered alongside converters via {@link ColumnRestrictionRegistry#register}. They allow the client
 * to proactively surface helpful error messages before a write is attempted, without requiring a round-trip to the
 * server.
 */
@JsFunction
public interface ColumnRestrictionValidator {
    /**
     * Validate a proposed value against this restriction.
     *
     * @param value The proposed column value
     * @param data The restriction's data object (structure depends on the restriction type)
     * @return An error message string if the value violates the restriction, or {@code null} if the value is valid
     */
    @JsNullable
    String validate(Object value, JsPropertyMap<Object> data);
}

