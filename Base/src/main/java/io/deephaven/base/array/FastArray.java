/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.array;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.Copyable;
import io.deephaven.base.Function;
import io.deephaven.base.verify.Assert;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Array;

public class FastArray<T> {
    protected final Class<? extends T> clazz;
    protected final Function.Nullary<? extends T> newInstance;

    protected int length;
    protected T[] array;

    public FastArray(final Class<? extends T> clazz) {
        this(clazz, new Function.Nullary<T>() {
            @Override
            public T call() {
                try {
                    return clazz.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public FastArray(final Class<? extends T> clazz, int initialSize) {
        this(clazz, new Function.Nullary<T>() {
            @Override
            public T call() {
                try {
                    return clazz.newInstance();
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }, initialSize, true);
    }

    // newInstance shouldn't use a pool or any special mechanism! (Sharing happens w/ newInstance w/ other cloned/copied
    // instances)
    // need to use clazz b/c we want array to actually be an array of type T
    public FastArray(final Class<? extends T> clazz, final Function.Nullary<? extends T> newInstance) {
        this(clazz, newInstance, 0, true);
    }

    public FastArray(final Class<? extends T> clazz, final Function.Nullary<? extends T> newInstance, int initialSize,
            boolean preallocate) {
        this.clazz = clazz;
        this.newInstance = newInstance;
        this.length = 0;
        this.array = (T[]) Array.newInstance(clazz, initialSize);

        if (preallocate) {
            for (int i = 0; i < initialSize; ++i) {
                next();
            }
            quickReset();
        }
    }

    public Function.Nullary<? extends T> getNewInstance() {
        return newInstance;
    }

    public FastArray(final Function.Nullary<? extends T> newInstance) {
        this((Class) newInstance.call().getClass(), newInstance);
    }

    public final void add(T t) {
        Assert.neqNull(t, "t");
        fastAdd(t);
    }

    public final void add(T[] t, int startIndex, int len) {
        for (int i = 0; i < len; ++i) {
            Assert.neqNull(t[startIndex + i], "t[startIndex + i]");
        }
        fastAdd(t, startIndex, len);
    }

    // useful for maintaining small sets based on object identity
    public final boolean addUnique(T t) {
        for (int i = 0; i < length; ++i) {
            if (array[i] == t) {
                return false;
            }
        }
        fastAdd(t);
        return true;
    }

    // useful when using FastArray w/ immutable objects. enums, string, etc. or when you must remember a previous value
    public final void fastAdd(T t) {
        array = ArrayUtil.put(array, length, t, clazz);
        ++length;
    }

    public final void fastAdd(T[] t, int startIndex, int len) {
        array = ArrayUtil.put(array, length, t, startIndex, len, clazz);
        length += len;
    }

    // useful when using FastArray as a cache / when FastArray is managing memory. must remember to reset or copy over
    // returned value
    public final T next() {
        T t;
        if ((length >= array.length) || ((t = array[length]) == null)) {
            t = newInstance.call();
        }
        fastAdd(t);
        return t;
    }

    public final T pop() {
        return length > 0 ? array[--length] : null;
    }

    // likely used in conjunction w/ next()
    public final void quickReset() {
        length = 0;
    }

    // should not be used when calling next()
    public final void normalReset() {
        normalReset(null);
    }

    // should not be used when calling next()
    public final void fullReset() {
        fullReset(null);
    }

    public final void normalReset(T resetValue) {
        for (int i = 0; i < length; ++i) {
            array[i] = resetValue;
        }
        length = 0;
    }

    public static <C extends Copyable<C>> void copyNormalReset(FastArray<C> THIS, C resetValue) {
        for (int i = 0; i < THIS.length; ++i) {
            THIS.array[i].copyValues(resetValue);
        }
        THIS.length = 0;
    }

    public final void fullReset(T resetValue) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = resetValue;
        }
        length = 0;
    }

    public static <C extends Copyable<C>> void copyFullReset(FastArray<C> THIS, C resetValue) {
        for (int i = 0; i < THIS.array.length; ++i) {
            THIS.array[i].copyValues(resetValue);
        }
        THIS.length = 0;
    }

    public final void arrayReset() {
        length = 0;
        array = (T[]) Array.newInstance(clazz, 0);
    }

    public final int getLength() {
        return length;
    }

    public final T[] getUnsafeArray() {
        return array;
    }

    public T removeThisIndex(int index) {
        if (index >= length) {
            throw new IllegalArgumentException(
                    "you tried to remove this index: " + index + " when the array is only this long: " + length);
        } else if (index < 0) {
            throw new IllegalArgumentException(
                    "you tried to remove this index: " + index + " when we can only remove positive indices");
        } else {

            final T t = array[index];

            // move all the items ahead one index and reduce the length
            for (int i = index; i < length - 1; i++) {
                array[i] = array[i + 1];
            }
            length--;

            array[length] = null;

            return t;
        }
    }

    public T removeThisIndexDontCareAboutOrder(int index) {
        if (index >= length) {
            throw new IllegalArgumentException(
                    "you tried to remove this index: " + index + " when the array is only this long: " + length);
        } else if (index < 0) {
            throw new IllegalArgumentException(
                    "you tried to remove this index: " + index + " when we can only remove positive indices");
        } else {

            final T t = array[index];
            length--;
            array[index] = array[length];
            array[length] = null;
            return t;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final FastArray other = (FastArray) o;

        if (clazz != null ? !clazz.equals(other.clazz) : other.clazz != null)
            return false;
        if (length != other.length)
            return false;
        if (!ArrayUtil.equals(array, other.array, 0, 0, length))
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = length;
        result = 31 * result + ArrayUtil.hashCode(array, 0, length);
        return result;
    }

    @Override
    public String toString() {
        return toStringXml("");
    }

    public String toStringXml(String pre) {
        StringBuilder msg = new StringBuilder();
        String extra = "   ";
        msg.append(pre).append("<FastArray>\n");
        msg.append(pre).append(extra).append("<length>").append(length).append("</length>\n");
        msg.append(pre).append(extra).append("<array>\n");
        for (int i = 0; i < array.length; i++) {
            msg.append(pre).append(extra).append(extra).append("<index>").append(i).append("</index>\n");
            msg.append(pre).append(extra).append(extra).append("<entry>\n");
            if (array[i] == null) {
                msg.append(pre).append(extra).append(extra).append(extra).append("null");
            } else {
                msg.append(array[i].toString());
            }
            msg.append(pre).append(extra).append("</entry>\n");
        }
        msg.append(pre).append(extra).append("</array>\n");
        msg.append(pre).append("</FastArray>\n");
        return msg.toString();
    }

    // shallow useful for objects that are immutable. enums, strings, etc.
    public static <C> void copyValuesShallow(final FastArray<C> THIS, final FastArray<C> right) {
        if (THIS != right) {
            THIS.length = right.length;
            THIS.array = ArrayUtil.ensureSizeNoCopy(THIS.array, THIS.length, THIS.clazz);
            System.arraycopy(right.array, 0, THIS.array, 0, THIS.length);
        }
    }

    public static <C> FastArray<C> cloneShallow(final FastArray<C> THIS) {
        FastArray<C> clone = new FastArray<C>(THIS.clazz, THIS.newInstance);
        copyValuesShallow(clone, THIS);
        return clone;
    }

    /**
     * @param THIS array will hold copies of right's content. Modified
     * @param right content holder. Not-modified
     * @param <C>
     */
    public static <C extends Copyable<C>> void copyValuesDeep(final FastArray<C> THIS, final FastArray<C> right) {
        Assert.eqTrue(maybeCopyValuesDeep(THIS, right), "maybeCopyValuesDeep(THIS, right)");
    }

    // Even if this returns true, it doesn't mean that it was successfully copied!
    // Right still might have changed... use other methods to detect
    // However, if it returns false, we know right has changed!
    public static <C extends Copyable<C>> boolean maybeCopyValuesDeep(final FastArray<C> THIS,
            final FastArray<C> right) {
        if (THIS == right)
            return true;

        THIS.length = right.length;
        THIS.array = ArrayUtil.ensureSize(THIS.array, THIS.length, THIS.clazz);
        for (int i = 0; i < THIS.length; ++i) {
            final C rightItem = right.array[i];
            if (rightItem == null)
                return false; // something changed on us!
            else if (THIS.array[i] == null)
                THIS.array[i] = rightItem.safeClone();
            else
                THIS.array[i].copyValues(rightItem);
        }
        return THIS.length == right.length;
    }

    public static <C extends Copyable<C>> FastArray<C> cloneDeep(final FastArray<C> THIS) {
        FastArray<C> clone = new FastArray<C>(THIS.clazz, THIS.newInstance);
        copyValuesDeep(clone, THIS);
        return clone;
    }

    public static <C extends Externalizable> void writeExternal(final FastArray<C> THIS, ObjectOutput out)
            throws IOException {
        writeExternal(THIS, out, THIS.getLength());
    }

    public static <C extends Externalizable> void writeExternal(final FastArray<C> THIS, ObjectOutput out,
            int maxToWrite) throws IOException {
        maxToWrite = Math.min(maxToWrite, THIS.getLength());
        out.writeInt(maxToWrite);
        for (int i = 0; i < maxToWrite; ++i) {
            THIS.array[i].writeExternal(out);
        }
    }

    public static <C extends Externalizable> void readExternal(final FastArray<C> THIS, ObjectInput in)
            throws IOException, ClassNotFoundException {
        THIS.quickReset();
        final int len = in.readInt();
        for (int i = 0; i < len; ++i) {
            THIS.next().readExternal(in);
        }
    }

    public static interface WriteExternalFunction<C> {
        public void writeExternal(final ObjectOutput out, final C item) throws IOException;
    }

    public static <C> void writeExternal(final FastArray<C> THIS, ObjectOutput out,
            WriteExternalFunction<C> writeExternalFunction) throws IOException {
        if (THIS == null) {
            throw new IllegalArgumentException("FastArray.writeExternal(): THIS was null and is not supported");
        }
        out.writeInt(THIS.length);
        for (int i = 0; i < THIS.length; ++i) {
            writeExternalFunction.writeExternal(out, THIS.array[i]);
        }
    }

    public static interface ReadExternalFunction<C> {
        public void readExternal(final ObjectInput in, final C item) throws IOException, ClassNotFoundException;
    }

    public static <C> void readExternal(final FastArray<C> THIS, ObjectInput in,
            ReadExternalFunction<C> readExternalFunction) throws IOException, ClassNotFoundException {
        if (THIS == null) {
            throw new IllegalArgumentException("FastArray.readExternal(): THIS was null and is not supported");
        }
        THIS.quickReset();
        final int len = in.readInt();
        for (int i = 0; i < len; ++i) {
            readExternalFunction.readExternal(in, THIS.next());
        }
    }
}
