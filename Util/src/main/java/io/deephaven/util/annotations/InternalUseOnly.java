package io.deephaven.util.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Target;

/**
 * Indicates that a particular method is for internal use only and should not be used by client code. It is subject to
 * change/removal at any time.
 */
@Target({ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE})
@Inherited
@Documented
public @interface InternalUseOnly {
}
