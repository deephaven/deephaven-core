package io.deephaven.web.client.api.i18n;

import com.google.gwt.i18n.client.NumberFormat;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Exported wrapper of the GWT NumberFormat, plus LongWrapper support
 */
@JsType(namespace = "dh.i18n", name = "NumberFormat")
public class JsNumberFormat {
    private static final Map<String, JsNumberFormat> cache = new HashMap<>();

    public static JsNumberFormat getFormat(String pattern) {
        return cache.computeIfAbsent(pattern, JsNumberFormat::new);
    }

    public static double parse(String pattern, String text) {
        return getFormat(pattern).parse(text);
    }

    public static String format(String pattern, Object number) {
        return getFormat(pattern).format(number);
    }

    private final String pattern;
    private final NumberFormat wrapped;

    @JsConstructor
    public JsNumberFormat(String pattern) {
        this.pattern = pattern;
        wrapped = NumberFormat.getFormat(pattern);
    }

    public double parse(String text) {
        return wrapped.parse(text);
    }

    public String format(Object number) {
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
