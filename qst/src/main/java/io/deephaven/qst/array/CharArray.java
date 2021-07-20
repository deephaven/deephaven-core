package io.deephaven.qst.array;

import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.PrimitiveType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public final class CharArray extends PrimitiveArrayBase<Character> {
    static final char NULL_REPR = Character.MAX_VALUE - 1;

    public static CharArray empty() {
        return new CharArray(new char[0]);
    }

    public static CharArray of(char... values) {
        return builder(values.length).add(values).build();
    }

    public static CharArray of(Character... values) {
        return builder(values.length).add(values).build();
    }

    public static CharArray of(Iterable<Character> values) {
        if (values instanceof Collection) {
            return of((Collection<Character>) values);
        }
        return builder(16).add(values).build();
    }

    public static CharArray of(Collection<Character> values) {
        return builder(values.size()).add(values).build();
    }

    public static CharArray ofUnsafe(char... values) {
        return new CharArray(values);
    }

    public static Builder builder(int initialSize) {
        return new Builder(initialSize);
    }

    private static char adapt(Character x) {
        return x == null ? NULL_REPR : x;
    }

    private static Character adapt(char x) {
        return x == NULL_REPR ? null : x;
    }

    private final char[] values;

    private CharArray(char[] values) {
        this.values = Objects.requireNonNull(values);
    }

    /**
     * The raw chars. Must not be modified.
     *
     * @return the chars, do <b>not</b> modify
     */
    public final char[] values() {
        return values;
    }

    @Override
    public final Character get(int index) {
        return adapt(values[index]);
    }

    @Override
    public final int size() {
        return values().length;
    }

    @Override
    public final PrimitiveType<Character> type() {
        return CharType.instance();
    }

    @Override
    public final <V extends PrimitiveArray.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        CharArray charArray = (CharArray) o;
        return Arrays.equals(values, charArray.values);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(values);
    }

    public static class Builder implements ArrayBuilder<Character, CharArray, Builder> {

        private char[] array;
        private int size;

        private Builder(int initialCapacity) {
            this.array = new char[initialCapacity];
            this.size = 0;
        }

        public synchronized final Builder add(char item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public synchronized final Builder add(char... items) {
            // todo: systemcopy
            for (char item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Character item) {
            return add(adapt(item));
        }

        @Override
        public synchronized final Builder add(Character... items) {
            for (Character item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final Builder add(Iterable<Character> items) {
            for (Character item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public synchronized final CharArray build() {
            return new CharArray(takeAtSize());
        }

        private void ensureCapacity() {
            if (size == array.length) {
                char[] next = new char[array.length == 0 ? 1 : array.length * 2];
                System.arraycopy(array, 0, next, 0, array.length);
                array = next;
            }
        }

        private char[] takeAtSize() {
            if (size == array.length) {
                return array; // great case, no copying necessary :)
            }
            char[] atSize = new char[size];
            System.arraycopy(array, 0, atSize, 0, size);
            return atSize;
        }
    }
}
