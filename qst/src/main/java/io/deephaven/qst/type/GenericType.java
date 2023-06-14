/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

/**
 * A non-primitive object type.
 *
 * @param <T> the object type
 * @see StringType
 * @see InstantType
 * @see ArrayType
 * @see CustomType
 */
public interface GenericType<T> extends Type<T> {

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {
        R visit(StringType stringType);

        R visit(InstantType instantType);

        R visit(ArrayType<?, ?> arrayType);

        // Implementation note: when adding new types here, add type to TypeHelper

        R visit(CustomType<?> customType);
    }
}
