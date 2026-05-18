//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionValidator;
// Imported for Javadoc @link references only
import io.deephaven.web.client.api.DoubleRangeColumnRestriction;
import io.deephaven.web.client.api.IntegerRangeColumnRestriction;
import io.deephaven.web.client.api.NonEmptyColumnRestriction;
import io.deephaven.web.client.api.NotNullColumnRestriction;
import io.deephaven.web.client.api.StringListColumnRestriction;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.base.JsPropertyMap;

/**
 * Represents a restriction on an input table column. Each restriction has a {@code type} string identifying what kind
 * of restriction it is.
 *
 * <p>
 * Built-in restriction types are exposed as typed subclasses with strongly-typed fields:
 * <ul>
 * <li>{@link IntegerRangeColumnRestriction} ({@code "IntegerRangeRestriction"})</li>
 * <li>{@link DoubleRangeColumnRestriction} ({@code "DoubleRangeRestriction"})</li>
 * <li>{@link NotNullColumnRestriction} ({@code "NotNullRestriction"})</li>
 * <li>{@link NonEmptyColumnRestriction} ({@code "NonEmptyRestriction"})</li>
 * <li>{@link StringListColumnRestriction} ({@code "StringListRestriction"})</li>
 * </ul>
 *
 * <p>
 * If the client encounters an unrecognized restriction type it will be represented as a plain {@code ColumnRestriction}
 * whose {@link #getData()} bag contains the raw fields. Additional restriction types can be registered via
 * {@code dh.registerRestrictionType}; the server will always enforce all restrictions regardless of whether the client
 * recognizes them.
 */
@TsName(namespace = "dh")
public class ColumnRestriction {
    private final String type;
    private final JsPropertyMap<Object> data;
    private ColumnRestrictionValidator validator;

    /**
     * Constructor for the generic / unknown-type fallback path.
     *
     * @param type The restriction type name (e.g., "IntegerRangeRestriction")
     * @param data An opaque JS object containing the type-specific restriction data; may be {@code null} for typed
     *        subclasses
     */
    public ColumnRestriction(String type, JsPropertyMap<Object> data) {
        this.type = type;
        this.data = data;
    }

    /**
     * Sets the client-side validator for this restriction. Used by the generic fallback path for custom registered
     * types. Not exposed to JavaScript — use {@link #validate(Object)} instead.
     *
     * @param validator The validator to attach
     */
    @JsIgnore
    public void setValidator(ColumnRestrictionValidator validator) {
        this.validator = validator;
    }

    /**
     * The type of restriction (e.g., {@code "IntegerRangeRestriction"}, {@code "StringListRestriction"}).
     *
     * @return The restriction type name
     */
    @JsProperty
    public String getType() {
        return type;
    }

    /**
     * An opaque data object whose structure is specific to the restriction {@link #getType() type}. This is populated
     * for unrecognized restriction types registered via {@code dh.registerRestrictionType}. For the built-in typed
     * subclasses this returns {@code null} — use the subclass's typed fields instead (e.g.,
     * {@link IntegerRangeColumnRestriction#getMin()}).
     *
     * @return The restriction data, or {@code null} for typed built-in subclasses
     */
    @JsProperty
    @JsNullable
    public JsPropertyMap<Object> getData() {
        return data;
    }

    /**
     * Validates a proposed value against this restriction. Returns {@code null} if the value is valid. For typed
     * built-in subclasses, validation is always available. For custom registered types, returns {@code null} when no
     * client-side validator was registered (the server will still enforce the restriction).
     *
     * @param value The proposed column value to validate
     * @return An error message if the value violates the restriction, or {@code null} if the value is valid (or no
     *         client-side validator is available for this type)
     */
    @JsMethod
    @JsNullable
    public String validate(Object value) {
        if (validator == null) {
            return null;
        }
        return validator.validate(value, data);
    }

    @Override
    public String toString() {
        return "ColumnRestriction{type='" + type + "', data=" + data + "}";
    }
}

