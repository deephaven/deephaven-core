//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to allow nulls in {@link org.immutables.value.Value.Immutable} collections.
 *
 * @see <a href="https://immutables.github.io/immutable.html#nulls-in-collection">nulls-in-collection</a>
 */
@Retention(RetentionPolicy.CLASS)
@Target({ElementType.METHOD})
public @interface AllowNulls {

}

