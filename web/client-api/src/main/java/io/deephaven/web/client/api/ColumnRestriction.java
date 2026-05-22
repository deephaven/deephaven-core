//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.Any;
import com.vertispan.tsdefs.annotations.TsName;
import io.deephaven.web.client.api.barrage.util.ColumnRestrictionConverterException;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;

import java.nio.ByteBuffer;

/**
 * Abstract base class representing a restriction on an input table column. Each restriction has a {@code type} string
 * identifying what kind of restriction it is, and a {@link #validate(jsinterop.base.Any)} method for client-side
 * validation.
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
 * Custom restriction types can be registered via {@code ColumnRestrictionRegistry.register}. The converter must return
 * a concrete subclass of {@code ColumnRestriction} that overrides {@link #validate(jsinterop.base.Any)} as needed.
 */
@TsName(namespace = "dh")
public abstract class ColumnRestriction {

    @FunctionalInterface
    public interface BufferParser<T extends ColumnRestriction> {
        T parse(ByteBuffer buffer) throws Exception;
    }

    protected static <T extends ColumnRestriction> T parseFromAny(Any restrictionAny, String type,
            BufferParser<T> parser) throws ColumnRestrictionConverterException {
        try {
            return parser.parse(restrictionAny.getValue().asReadOnlyByteBuffer());
        } catch (Exception e) {
            throw new ColumnRestrictionConverterException("Failed to convert " + type, e);
        }
    }

    private final String type;

    protected ColumnRestriction(String type) {
        this.type = type;
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
     * Validates a proposed value against this restriction.
     *
     * @param value The proposed column value to validate
     * @return An error message if the value violates the restriction, or {@code null} if the value is valid
     */
    @JsMethod
    @JsNullable
    public abstract String validate(jsinterop.base.Any value);

    @Override
    public String toString() {
        return "ColumnRestriction{type='" + type + "'}";
    }
}
