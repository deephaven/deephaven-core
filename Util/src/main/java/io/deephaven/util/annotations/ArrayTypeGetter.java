package io.deephaven.util.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation marks a specific method within a class that has been annotated with
 * {@link ArrayType} as the means to retrieve an array of the type indicated. No guarantees are
 * provided with respect to the mutability of the annotated type..
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ArrayTypeGetter {
}
