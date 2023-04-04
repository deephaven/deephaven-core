/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.i18n;

import com.google.gwt.i18n.client.NumberFormat;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.web.client.api.BigDecimalWrapper;
import io.deephaven.web.client.api.BigIntegerWrapper;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
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

    public static JsNumberFormat getFormat(String pattern) {
        return cache.computeIfAbsent(pattern, JsNumberFormat::new);
    }

    public static double parse(String pattern, String text) {
        return getFormat(pattern).parse(text);
    }

    public static String format(String pattern, NumberUnion number) {
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
