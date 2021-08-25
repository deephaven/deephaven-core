package io.deephaven.util.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * This annotation indicates that a field is used in dynamically generated code.
 */
/*
 * IntelliJ must be configured to recognize this annotation and suppress warnings. The applicable settings are
 * Preferences -> Editors -> Inspections: -- Unused Declarations -> Entry Points -> Annotations -> Add it to the list --
 * Field Can Be Local -> Additional Special Annotations -> Add it to the list.
 */
@Documented
@Target({ElementType.FIELD, ElementType.LOCAL_VARIABLE, ElementType.METHOD})
public @interface DynamicUse {
    String[] locations() default "";
}
