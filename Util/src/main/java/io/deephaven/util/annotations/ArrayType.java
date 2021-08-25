package io.deephaven.util.annotations;

import java.lang.annotation.*;

/**
 * This annotation indicates that the annotated class in some way represents an array of the specified type.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ArrayType {
    /**
     * The array type. Note this is in the form of T[] not the component type.
     * 
     * @return the array type.
     */
    Class type();
}
