//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.annotations;

import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style.ImplementationVisibility;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A style for objects that should declare a builder interface to use for construction. Disables
 * {@link Immutable#copy()}. Recommended for objects with more than two fields, or default fields.
 */
@Target({ElementType.TYPE, ElementType.PACKAGE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(visibility = ImplementationVisibility.PACKAGE,
        defaults = @Value.Immutable(copy = false),
        strictBuilder = true,
        weakInterning = true,
        jdkOnly = true,
        includeHashCode = "getClass().hashCode()")
public @interface BuildableStyle {
    // Note: this produces ImmutableX.builder()s for the implementation classes
}
