//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.object;

import java.util.Objects;

/**
 * A union object represents a {@link #of(boolean) boolean}, {@link #of(char) char}, {@link #of(byte) byte},
 * {@link #of(short) short}, {@link #of(int) int}, {@link #of(long) long}, {@link #of(float) float}, {@link #of(double)
 * double}, or {@link #of(Object) Object}. It is meant to provide a more strongly typed alternative in cases where
 * {@link Object Objects} would otherwise be used.
 */
public interface UnionObject {

    static UnionObject of(boolean x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static UnionObject of(char x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static UnionObject of(byte x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static UnionObject of(short x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static UnionObject of(int x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static UnionObject of(long x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static UnionObject of(float x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static UnionObject of(double x) {
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    /**
     * Create a wrapped object, must not be a boxed primitive type. Use {@link #from(Object)} if you need boxed
     * primitive support.
     *
     * @param x the object
     * @return the union object
     */
    static UnionObject of(Object x) {
        Objects.requireNonNull(x);
        if (UnionObjectBase.isBoxedPrimitive(x.getClass())) {
            throw new IllegalArgumentException(String.format(
                    "Object is boxed type %s, must use UnionObject#from, or more appropriate primitive method #of",
                    x.getClass()));
        }
        return new UnionObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    /**
     * If {@code x} is a boxed primitive, this will create the union object with the primitive value. Otherwise, this
     * will create the object via {@link #of(Object)}.
     *
     * @param x the object
     * @return the union object
     */
    static UnionObject from(Object x) {
        if (x instanceof Boolean) {
            return of((boolean) x);
        }
        if (x instanceof Character) {
            return of((char) x);
        }
        if (x instanceof Byte) {
            return of((byte) x);
        }
        if (x instanceof Short) {
            return of((short) x);
        }
        if (x instanceof Integer) {
            return of((int) x);
        }
        if (x instanceof Long) {
            return of((long) x);
        }
        if (x instanceof Float) {
            return of((float) x);
        }
        if (x instanceof Double) {
            return of((double) x);
        }
        return of(x);
    }

    /**
     * Unwraps the object or boxed primitive.
     *
     * @return the object
     */
    Object unwrap();

    /**
     * Returns the object as type {@code T} if the unwrapped object is an instance of {@code clazz}.
     *
     * @param clazz the class
     * @return this object as type of clazz
     * @param <T> the type
     * @throws IllegalArgumentException if the object is not an instance of {@code clazz}.
     */
    <T> T expect(Class<T> clazz) throws IllegalArgumentException;

    /**
     * Equivalent to {@code expect(Number.class)}.
     *
     * @return this object as a {@link Number}
     * @throws IllegalArgumentException if the object is not an instance of {@link Number}.
     * @see #expect(Class)
     */
    Number number() throws IllegalArgumentException;

    /**
     * Equivalent to {@code expect(Boolean.class)}.
     *
     * @return this object as a boolean
     * @see #expect(Class)
     */
    boolean booleanValue();

    /**
     * Equivalent to {@code expect(Character.class)}.
     *
     * @return this object as a character
     * @see #expect(Class)
     */
    char charValue();

    /**
     * Equivalent to {@code number().byteValue()}.
     *
     * @return this object as a byte
     * @see #number()
     */
    byte byteValue();

    /**
     * Equivalent to {@code number().shortValue()}.
     *
     * @return this object as a short
     * @see #number()
     */
    short shortValue();

    /**
     * Equivalent to {@code number().intValue()}.
     *
     * @return this object as an int
     * @see #number()
     */
    int intValue();

    /**
     * Equivalent to {@code number().longValue()}.
     *
     * @return this object as a long
     * @see #number()
     */
    long longValue();

    /**
     * Equivalent to {@code number().floatValue()}.
     *
     * @return this object as a float
     * @see #number()
     */
    float floatValue();

    /**
     * Equivalent to {@code number().doubleValue()}.
     *
     * @return this object as a double
     * @see #number()
     */
    double doubleValue();

    <T> T visit(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(boolean x);

        T visit(char x);

        T visit(byte x);

        T visit(short x);

        T visit(int x);

        T visit(long x);

        T visit(float x);

        T visit(double x);

        T visit(Object x);
    }
}
