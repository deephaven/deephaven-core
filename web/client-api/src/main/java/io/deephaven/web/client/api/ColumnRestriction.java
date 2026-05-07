//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionValidator;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.base.JsPropertyMap;

/**
 * Represents a restriction on an input table column. Each restriction has a {@code type} string identifying what kind
 * of restriction it is, and a {@code data} object whose structure depends on the type.
 *
 * <p>
 * Known built-in types and their {@code data} shapes:
 * <ul>
 * <li>{@code "IntegerRangeRestriction"} - {@code {min: number, max: number}}</li>
 * <li>{@code "DoubleRangeRestriction"} - {@code {min: number, max: number}}</li>
 * <li>{@code "NotNullRestriction"} - {@code {}}</li>
 * <li>{@code "NonEmptyRestriction"} - {@code {}}</li>
 * <li>{@code "StringListRestriction"} - {@code {values: string[]}}</li>
 * </ul>
 *
 * <p>
 * Additional restriction types can be registered via {@code dh.registerRestrictionType}. If the client encounters a
 * type it does not recognize, it will still pass the restriction through to the server for validation, but cannot
 * provide proactive client-side error messages.
 */
@TsName(namespace = "dh")
public class ColumnRestriction {
    private final String type;
    private final JsPropertyMap<Object> data;
    private ColumnRestrictionValidator validator;

    /**
     * @param type The restriction type name (e.g., "IntegerRangeRestriction")
     * @param data An opaque JS object containing the type-specific restriction data
     */
    public ColumnRestriction(String type, JsPropertyMap<Object> data) {
        this.type = type;
        this.data = data;
    }

    /**
     * Sets the client-side validator for this restriction. Called internally when a registered validator is found for
     * the restriction type. Not exposed to JavaScript — use {@link #validate(Object)} instead.
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
     * An opaque data object whose structure is specific to the restriction {@link #getType() type}. Known built-in
     * shapes are documented on the class.
     *
     * @return The restriction data
     */
    @JsProperty
    public JsPropertyMap<Object> getData() {
        return data;
    }

    /**
     * Returns {@code true} if this restriction has a registered client-side validator. When {@code false},
     * {@link #validate(Object)} will always return {@code null}, but the server will still enforce the restriction.
     *
     * @return Whether a client-side validator is available
     */
    @JsProperty(name = "hasValidator")
    public boolean hasValidator() {
        return validator != null;
    }

    /**
     * Validates a proposed value against this restriction. Returns {@code null} either if the value is valid
     * <em>or</em> if no client-side validator is registered — use {@link #hasValidator()} to distinguish the two cases.
     *
     * @param value The proposed column value to validate
     * @return An error message if the value violates the restriction, or {@code null} if the value is valid or no
     *         validator is registered
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

