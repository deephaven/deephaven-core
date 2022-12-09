package io.deephaven.api.object;

import java.util.Objects;

public interface AnnotatedObject {

    static AnnotatedObject of(boolean x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(char x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(byte x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(short x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(int x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(long x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(float x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(double x) {
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    static AnnotatedObject of(Object x) {
        Objects.requireNonNull(x);
        if (AnnotatedObjectBase.isBoxedPrimitive(x.getClass())) {
            throw new IllegalArgumentException(String.format(
                    "Object is boxed type %s, must use AnnotatedObject#from, or more appropriate primitive method #of",
                    x.getClass()));
        }
        return new AnnotatedObjectBase() {
            @Override
            public <T> T visit(Visitor<T> visitor) {
                return visitor.visit(x);
            }
        };
    }

    /**
     * If {@code x} is a boxed primitive, this will create the appropriate object with the unboxed value. Otherwise,
     * this will create the object via {@link #of(Object)}.
     *
     * @param x the object
     * @return the annotated object
     */
    static AnnotatedObject from(Object x) {
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

    Object unwrap();

    /**
     * Equivalent to {@code expect(Number.class)}.
     *
     * @return this object as a {@link Number}
     */
    Number number();

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

    /**
     * Unwraps the object as type {@code T} if the object is an instance of {@code clazz}.
     *
     * @param clazz the class
     * @return this object as type of clazz
     * @param <T> the type
     */
    <T> T expect(Class<T> clazz);

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
