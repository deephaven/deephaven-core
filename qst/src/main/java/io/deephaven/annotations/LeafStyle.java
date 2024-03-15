//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.annotations;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A leaf node style is suitable for leaf nodes.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(visibility = ImplementationVisibility.PACKAGE,
        defaults = @Value.Immutable(copy = false),
        strictBuilder = true,
        weakInterning = true,
        includeHashCode = "getClass().hashCode()")
public @interface LeafStyle {
}
