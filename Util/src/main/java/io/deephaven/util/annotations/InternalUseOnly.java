//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Target;

/**
 * Indicates that a particular {@link ElementType#METHOD method}, {@link ElementType#CONSTRUCTOR constructor},
 * {@link ElementType#TYPE type}, or {@link ElementType#PACKAGE package} is for internal use only and should not be used
 * by client code. It is subject to change/removal at any time.
 */
@Target({ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE, ElementType.PACKAGE})
@Inherited
@Documented
public @interface InternalUseOnly {
}
