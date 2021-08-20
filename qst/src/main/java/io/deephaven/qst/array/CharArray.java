package io.deephaven.qst.array;

import io.deephaven.qst.type.CharType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * A {@link CharType} array.
 */
public final class CharArray extends PrimitiveArrayBase<Character> {

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
        return builder(Util.DEFAULT_BUILDER_INITIAL_CAPACITY).add(values).build();
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
    public final int size() {
        return values().length;
    }

    @Override
    public final CharType componentType() {
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

    public static class Builder extends PrimitiveArrayHelper<char[]>
        implements ArrayBuilder<Character, CharArray, Builder> {

        private Builder(int initialCapacity) {
            super(new char[initialCapacity]);
        }

        @Override
        int length(char[] array) {
            return array.length;
        }

        @Override
        void arraycopy(char[] src, int srcPos, char[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        char[] construct(int size) {
            return new char[size];
        }

        public final Builder add(char item) {
            ensureCapacity();
            array[size++] = item;
            return this;
        }

        public final Builder add(char... items) {
            addImpl(items);
            return this;
        }

        @Override
        public final Builder add(Character item) {
            return add(Util.adapt(item));
        }

        private void addInternal(Character item) {
            array[size++] = Util.adapt(item);
        }

        @Override
        public final Builder add(Character... items) {
            ensureCapacity(items.length);
            for (Character item : items) {
                addInternal(item);
            }
            return this;
        }

        @Override
        public final Builder add(Iterable<Character> items) {
            for (Character item : items) {
                add(item);
            }
            return this;
        }

        @Override
        public final CharArray build() {
            return new CharArray(takeAtSize());
        }
    }
}
