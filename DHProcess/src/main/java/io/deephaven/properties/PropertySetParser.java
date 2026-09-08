//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.properties;

// TODO: - not in use yet
public interface PropertySetParser<T> {
    T parse(PropertySet properties);
}
