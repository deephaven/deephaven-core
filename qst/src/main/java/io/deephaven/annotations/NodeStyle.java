package io.deephaven.annotations;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The node style is suitable for nested / recursive structures. As such, it is prehashed and
 * interned.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(visibility = ImplementationVisibility.PACKAGE,
    defaults = @Value.Immutable(prehash = true, intern = true), strictBuilder = true,
    weakInterning = true)
public @interface NodeStyle {
}
