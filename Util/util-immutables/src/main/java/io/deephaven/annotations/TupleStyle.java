package io.deephaven.annotations;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * A tuple style is for objects that represent simple tuples, with all parameters specified at construction. Not
 * recommended for objects with more than two fields. Not applicable for objects with default fields.
 */
@Target({ElementType.TYPE, ElementType.PACKAGE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(visibility = ImplementationVisibility.PACKAGE,
        allParameters = true,
        typeImmutable = "*Tuple",
        defaults = @Value.Immutable(builder = false, copy = false), strictBuilder = true,
        weakInterning = true, jdkOnly = true)
public @interface TupleStyle {
    // Note: this produces XTuple.of() methods for the implementation classes
}
