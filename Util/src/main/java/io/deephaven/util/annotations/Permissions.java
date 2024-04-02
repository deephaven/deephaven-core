//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Target;

/**
 * <p>
 * This annotation indicates that a class or method is created by the Java Security Manager.
 * </p>
 * <p>
 * Classes and methods with this annotation should be <b>public</b>
 * </p>
 */
@Target({ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE})
@Inherited
@Documented
public @interface Permissions {
}
