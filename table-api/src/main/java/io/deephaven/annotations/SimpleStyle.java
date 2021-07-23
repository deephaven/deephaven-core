package io.deephaven.annotations;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A simple style is for objects that are simple to build. Not recommended for objects with more
 * than two fields. Not applicable for objects with default fields.
 */
@Target({ElementType.TYPE, ElementType.PACKAGE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(visibility = ImplementationVisibility.PACKAGE,
    defaults = @Value.Immutable(builder = false, copy = false), strictBuilder = true,
    weakInterning = true)
public @interface SimpleStyle {
    // Note: this produces ImmutableX.of() methods for the implementation classes
}
