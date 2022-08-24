/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.util;

import java.lang.annotation.*;

/**
 * Indicates that the annotated method should be executed concurrently with respect to the {@code UpdateGraphProcessor}
 * (UGP). Concurrent execution will not acquire the UGP lock before invocation, and will be run concurrently with other
 * annotated methods whenever possible.
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConcurrentMethod {
}
