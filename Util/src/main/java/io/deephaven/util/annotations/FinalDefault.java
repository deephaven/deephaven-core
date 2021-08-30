package io.deephaven.util.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Target;

/**
 * Indicates that a defaulted method should be treated as final and never overridden.
 */
@Target({ElementType.METHOD})
@Inherited
@Documented
public @interface FinalDefault {
}
