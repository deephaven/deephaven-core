package io.deephaven.api.object;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

abstract class UnionObjectBase implements UnionObject {

    private static final Set<Class<?>> BOXED_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            Boolean.class,
            Character.class,
            Byte.class,
            Short.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class)));

    static boolean isBoxedPrimitive(Class<?> clazz) {
        return BOXED_TYPES.contains(clazz);
    }

    @Override
    public final Object unwrap() {
        return visit(Unwrap.INSTANCE);
    }

    @Override
    public final Number number() {
        return expect(Number.class);
    }

    @Override
    public final boolean booleanValue() {
        return expect(Boolean.class);
    }

    @Override
    public final char charValue() {
        return expect(Character.class);
    }

    @Override
    public final byte byteValue() {
        return number().byteValue();
    }

    @Override
    public final short shortValue() {
        return number().shortValue();
    }

    @Override
    public final int intValue() {
        return number().intValue();
    }

    @Override
    public final long longValue() {
        return number().longValue();
    }

    @Override
    public final float floatValue() {
        return number().floatValue();
    }

    @Override
    public final double doubleValue() {
        return number().doubleValue();
    }

    @Override
    public final <T> T expect(Class<T> clazz) {
        return visit(new Expect<>(clazz));
    }

    enum Unwrap implements Visitor<Object> {
        INSTANCE;

        @Override
        public Object visit(boolean x) {
            return x;
        }

        @Override
        public Object visit(char x) {
            return x;
        }

        @Override
        public Object visit(byte x) {
            return x;
        }

        @Override
        public Object visit(short x) {
            return x;
        }

        @Override
        public Object visit(int x) {
            return x;
        }

        @Override
        public Object visit(long x) {
            return x;
        }

        @Override
        public Object visit(float x) {
            return x;
        }

        @Override
        public Object visit(double x) {
            return x;
        }

        @Override
        public Object visit(Object x) {
            return x;
        }
    }

    private static class Expect<T> implements Visitor<T> {
        private final Class<T> clazz;

        public Expect(Class<T> clazz) {
            this.clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public T visit(boolean x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("boolean is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(char x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("char is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(byte x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("byte is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(short x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("short is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(int x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("int is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(long x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("long is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(float x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("float is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(double x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("double is not %s", clazz));
            }
            return clazz.cast(x);
        }

        @Override
        public T visit(Object x) {
            if (!clazz.isInstance(x)) {
                throw new IllegalArgumentException(String.format("Object %s is not %s", x.getClass(), clazz));
            }
            return clazz.cast(x);
        }
    }
}
