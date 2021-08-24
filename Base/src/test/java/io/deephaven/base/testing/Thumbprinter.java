/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

// --------------------------------------------------------------------
/**
 * Calculates a short thumbprint of a complex class for use in unit testing.
 */
public interface Thumbprinter<T> {
    Thumbprinter DEFAULT = new Thumbprinter() {
        public String getThumbprint(Object object) {
            return null == object ? "null" : object.toString();
        }
    };

    String getThumbprint(T t);
}
