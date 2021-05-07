package io.deephaven.properties;

// TODO: - not in use yet
public interface PropertySetParser<T> {
    T parse(PropertySet properties);
}
