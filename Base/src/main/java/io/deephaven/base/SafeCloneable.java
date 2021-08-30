/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

/**
 * This interface specifies a safe clone operation that never throws a CloneNotSupported exception,
 * and also allows a bound to be placed on the result object's type.
 *
 * Note that any class that extends a base that implements SafeCloneable must *always* re-implement
 * the safeClone method.
 */
public interface SafeCloneable<T> extends Cloneable {
    T safeClone();
}
