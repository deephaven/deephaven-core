/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.i18n;

import com.google.gwt.i18n.client.NumberFormat;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Exported wrapper of the GWT NumberFormat, plus LongWrapper support
 *
 * Utility class to parse and format numbers, using the same format patterns as are supported by the standard Java
 * implementation used in the Deephaven server and swing client. Works for numeric types including BigInteger and
 * BigDecimal.
 */
@JsType(namespace = "dh.i18n", name = "NumberFormat")
public class JsNumberFormat {
    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    public interface NumberUnion {
        @JsOverlay
        default boolean isNumber() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        default boolean isBigInteger() {
            return this instanceof BigIntegerWrapper;
        }

        @JsOverlay
        default boolean isBigDecimal() {
            return this instanceof BigDecimalWrapper;
        }

        @JsOverlay
        default boolean isLongWrapper() {
            return this instanceof LongWrapper;
        }

        @TsUnionMember
        @JsOverlay
        default double asNumber() {
            return Js.asDouble(this);
        }

        @TsUnionMember
        @JsOverlay
        default BigIntegerWrapper asBigInteger() {
            return Js.cast(this);
        }

        @TsUnionMember
        @JsOverlay
        default BigDecimalWrapper asBigDecimal() {
            return Js.cast(this);
        }

        @TsUnionMember
        @JsOverlay
        default LongWrapper asLongWrapper() {
            return Js.cast(this);
        }
    }

    private static final Map<String, JsNumberFormat> cache = new HashMap<>();

    /**
     * a number format instance matching the specified format. If this format has not been specified before, a new
     * instance will be created and cached for later reuse. Prefer this method to calling the constructor directly to
     * take advantage of caching
     * 
     * @param pattern
     * @return dh.i18n.NumberFormat
     */
    public static JsNumberFormat getFormat(String pattern) {
        return cache.computeIfAbsent(pattern, JsNumberFormat::new);
    }

    /**
     * Parses the given text using the cached format matching the given pattern.
     *
     * @param pattern
     * @param text
     * @return double
     */
    public static double parse(String pattern, String text) {
        return getFormat(pattern).parse(text);
    }

    /**
     * Formats the specified number (or Java <b>long</b>, <b>BigInteger</b> or <b>BigDecimal</b> value) using the cached
     * format matching the given pattern string.
     *
     * @param pattern
     * @param number
     * @return String
     */
    public static String format(String pattern, NumberUnion number) {
        return getFormat(pattern).format(number);
    }

    private final String pattern;
    private final NumberFormat wrapped;

    /**
     * Creates a new number format instance. This generally should be avoided in favor of the static `getFormat`
     * function, which will create and cache an instance so that later calls share the same instance.
     *
     * @param pattern
     */
    public JsNumberFormat(String pattern) {
        this.pattern = pattern;
        wrapped = NumberFormat.getFormat(pattern);
    }

    /**
     * Parses the given text using this instance's pattern into a JS Number.
     *
     * @param text
     * @return double
     */
    public double parse(String text) {
        return wrapped.parse(text);
    }

    @JsIgnore
    public String format(double number) {
        return format(Js.<NumberUnion>cast(number));
    }

    @JsIgnore
    public String format(LongWrapper number) {
        return format(Js.<NumberUnion>cast(number));
    }

    /**
     * Formats the specified number (or Java `long`, `BigInteger` or `BigDecimal` value) using this instance's pattern.
     *
     * @param number
     * @return String
     */
    public String format(NumberUnion number) {
        Objects.requireNonNull(number);
        if (number.isNumber()) {// aka typeof number, and non-null
            return wrapped.format(number.asNumber());
        } else if (number.isBigDecimal()) {
            return wrapped.format(number.asBigDecimal().getWrapped());
        } else if (number.isBigInteger()) {
            return wrapped.format(number.asBigInteger().getWrapped());
        } else if (number.isLongWrapper()) {
            return wrapped.format(number.asLongWrapper().getWrapped());
        }
        throw new IllegalStateException("Can't format non-number object of type " + Js.typeof(number));
    }

    @Override
    public String toString() {
        return "NumberFormat { pattern='" + pattern + "' }";
    }
}
