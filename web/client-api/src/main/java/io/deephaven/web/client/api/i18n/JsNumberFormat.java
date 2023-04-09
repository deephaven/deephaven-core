/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.i18n;

import com.google.gwt.i18n.client.NumberFormat;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.math.BigDecimal;
import java.math.BigInteger;
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
    private static final Map<String, JsNumberFormat> cache = new HashMap<>();

    /**
     * @param pattern
     * @return a number format instance matching the specified format. If this format has not been specified before, a
     *         new instance will be created and cached for later reuse. Prefer this method to calling the constructor
     *         directly to take advantage of caching.
     */
    public static JsNumberFormat getFormat(String pattern) {
        return cache.computeIfAbsent(pattern, JsNumberFormat::new);
    }

    /**
     * Parses the given text using the cached format matching the given pattern.
     * 
     * @param pattern
     * @param text
     * @return
     */
    public static double parse(String pattern, String text) {
        return getFormat(pattern).parse(text);
    }

    /**
     * Formats the specified number (or Java `long`, `BigInteger` or `BigDecimal` value) using the cached format
     * matching the given pattern string.
     * 
     * @param pattern
     * @param number
     * @return
     */
    public static String format(String pattern, Any number) {
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
    @JsConstructor
    public JsNumberFormat(String pattern) {
        this.pattern = pattern;
        wrapped = NumberFormat.getFormat(pattern);
    }

    /**
     * Parses the given text using this instance's pattern into a JS Number.
     * 
     * @param text
     * @return
     */
    public double parse(String text) {
        return wrapped.parse(text);
    }

    /**
     * Formats the specified number (or Java `long`, `BigInteger` or `BigDecimal` value) using this instance's pattern.
     * 
     * @param number
     * @return
     */
    public String format(@TsTypeRef(Any.class) Object number) {
        Objects.requireNonNull(number);
        if (number instanceof Double) {// aka typeof number, and non-null
            return wrapped.format((double) (Double) number);
        } else if (number instanceof BigDecimalWrapper) {
            return wrapped.format(((BigDecimalWrapper) number).getWrapped());
        } else if (number instanceof BigIntegerWrapper) {
            return wrapped.format(((BigIntegerWrapper) number).getWrapped());
        } else if (number instanceof LongWrapper) {
            return wrapped.format((Long) ((LongWrapper) number).getWrapped());
        }
        throw new IllegalStateException("Can't format non-number object of type " + Js.typeof(number));
    }

    @Override
    public String toString() {
        return "NumberFormat { pattern='" + pattern + "' }";
    }
}
