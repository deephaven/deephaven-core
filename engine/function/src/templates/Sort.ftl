
package io.deephaven.function;

import io.deephaven.vector.*;
import io.deephaven.function.comparators.NullNaNAwareComparator;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.IntStream;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Basic.isNull;

/**
 * Functions for sorting primitive types.
 */
public class Sort {

    //////////////////////////// Object ////////////////////////////


    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> T[] sortObj(final ObjectVector<T> values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }
        if (values.isEmpty()) {
            return values.toArray();
        }

        final T[] vs = values.copyToArray();
        Arrays.sort(vs, comparator);
        return vs;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> T[] sortObj(final ObjectVector<T> values) {
        return sortObj(values, new NullNaNAwareComparator<>());
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> T[] sortObj(final T[] values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }

        final T[] copy = Arrays.copyOf(values, values.length);
        if (copy.length == 0) {
            return copy;
        }

        Arrays.sort(copy, comparator);
        return copy;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    @SafeVarargs
    static public <T extends Comparable<? super T>> T[] sortObj(final T... values) {
        return sortObj(values, new NullNaNAwareComparator<>());
    }

    /**
     * Returns the indices of values sorted from smallest to largest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted indices.
     */
    static public <T extends Comparable<? super T>> int[] sortIndexObj(final ObjectVector<T> values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }
        if (values.isEmpty()) {
            return new int[]{};
        }

        return IntStream.range(0, values.intSize("sortIndex"))
            .boxed().sorted((i, j) -> comparator.compare(values.get(i),values.get(j)) )
            .mapToInt(ele -> ele).toArray();
    }

    /**
     * Returns the indices of values sorted from smallest to largest.
     *
     * @param values values.
     * @return sorted indices.
     */
    static public <T extends Comparable<? super T>> int[] sortIndexObj(final ObjectVector<T> values) {
        return sortIndexObj(values, new NullNaNAwareComparator<>());
    }

    /**
     * Returns the indices of values sorted from smallest to largest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted indices.
     */
    static public <T extends Comparable<? super T>> int[] sortIndexObj(final T[] values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }

        return IntStream.range(0, values.length)
            .boxed().sorted((i, j) -> comparator.compare(values[i],values[j]) )
            .mapToInt(ele -> ele).toArray();
    }

    /**
     * Returns the indices of values sorted from smallest to largest.
     *
     * @param values values.
     * @return sorted indices.
     */
    @SafeVarargs
    static public <T extends Comparable<? super T>> int[] sortIndexObj(final T... values) {
        return sortIndexObj(values, new NullNaNAwareComparator<>());
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> T[] sortDescendingObj(final ObjectVector<T> values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return values.toArray();
        }

        final T[] vs = values.copyToArray();
        Arrays.sort(vs, comparator.reversed());
        return vs;
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> T[] sortDescendingObj(final ObjectVector<T> values) {
        return sortDescendingObj(values, new NullNaNAwareComparator<>());
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted values.
     */
    static public <T extends Comparable<? super T>> T[] sortDescendingObj(final T[] values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }

        return sortDescendingObj(new ObjectVectorDirect<>(values), comparator);
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    @SafeVarargs
    static public <T extends Comparable<? super T>> T[] sortDescendingObj(final T... values) {
        return sortDescendingObj(values, new NullNaNAwareComparator<>());
    }

    /**
     * Returns the indices of values sorted from largest to smallest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted indices.
     */
    static public <T extends Comparable<? super T>> int[] sortDescendingIndexObj(final ObjectVector<T> values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new int[]{};
        }

        return IntStream.range(0, values.intSize("sortIndex"))
            .boxed().sorted((i, j) -> -comparator.compare(values.get(i),values.get(j)) )
            .mapToInt(ele -> ele).toArray();
    }

    /**
     * Returns the indices of values sorted from largest to smallest.
     *
     * @param values values.
     * @return sorted indices.
     */
    static public <T extends Comparable<? super T>> int[] sortDescendingIndexObj(final ObjectVector<T> values) {
        return sortDescendingIndexObj(values, new NullNaNAwareComparator<>());
    }

    /**
     * Returns the indices of values sorted from largest to smallest.
     *
     * @param values values.
     * @param comparator value comparator.
     * @return sorted indices.
     */
    static public <T extends Comparable<? super T>> int[] sortDescendingIndexObj(final T[] values, final Comparator<T> comparator) {
        if (values == null) {
            return null;
        }

        return sortDescendingIndexObj(new ObjectVectorDirect<>(values), comparator);
    }

    /**
     * Returns the indices of values sorted from largest to smallest.
     *
     * @param values values.
     * @return sorted indices.
     */
    @SafeVarargs
    static public <T extends Comparable<? super T>> int[] sortDescendingIndexObj(final T... values) {
        return sortDescendingIndexObj(values, new NullNaNAwareComparator<>());
    }

    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    //////////////////////////// ${pt.primitive} ////////////////////////////


    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sort(final ${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return values.toArray();
        }

        final ${pt.primitive}[] vs = values.copyToArray();
        Arrays.sort(vs);
        return vs;
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sort(final ${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return sort(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sort(final ${pt.boxed}[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new ${pt.primitive}[]{};
        }

        final ${pt.primitive}[] vs = new ${pt.primitive}[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = isNull(values[i]) ? ${pt.null} : values[i];
        }

        Arrays.sort(vs);
        return vs;
    }

    /**
     * Returns the indices of values sorted from smallest to largest.
     *
     * @param values values.
     * @return sorted indices.
     */
    public static int[] sortIndex(final ${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new int[]{};
        }

        return IntStream.range(0, values.intSize("sortIndex"))
            .boxed().sorted((i, j) -> ${pt.boxed}.compare(values.get(i),values.get(j)) )
            .mapToInt(ele -> ele).toArray();
    }

    /**
     * Returns the indices of values sorted from smallest to largest.
     *
     * @param values values.
     * @return sorted indices.
     */
    public static int[] sortIndex(final ${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return sortIndex(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the indices of values sorted from smallest to largest.
     *
     * @param values values.
     * @return sorted indices.
     */
    public static int[] sortIndex(final ${pt.boxed}[] values) {
        if (values == null) {
            return null;
        }

        if (values.length == 0) {
            return new int[]{};
        }

        final ${pt.primitive}[] vs = new ${pt.primitive}[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = isNull(values[i]) ? ${pt.null} : values[i];
        }

        return sortIndex(new ${pt.vectorDirect}(vs));
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sortDescending(final ${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return values.toArray();
        }

        final ${pt.primitive}[] vs = values.copyToArray();
        Arrays.sort(vs);
        ArrayUtils.reverse(vs);

        return vs;
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sortDescending(final ${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return sortDescending(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sortDescending(final ${pt.boxed}[] values) {
        if (values == null) {
            return null;
        }

        final ${pt.primitive}[] result = sort(values);
        ArrayUtils.reverse(result);
        return result;
    }

    /**
     * Returns the indices of values sorted from largest to smallest.
     *
     * @param values values.
     * @return sorted indices.
     */
    public static int[] sortDescendingIndex(final ${pt.vector} values) {
        if (values == null) {
            return null;
        }

        if (values.isEmpty()) {
            return new int[]{};
        }

        return IntStream.range(0, values.intSize("sortIndex"))
            .boxed().sorted((i, j) -> -${pt.boxed}.compare(values.get(i),values.get(j)) )
            .mapToInt(ele -> ele).toArray();
    }

    /**
     * Returns the indices of values sorted from largest to smallest.
     *
     * @param values values.
     * @return sorted indices.
     */
    public static int[] sortDescendingIndex(final ${pt.primitive}... values) {
        if (values == null) {
            return null;
        }

        return sortDescendingIndex(new ${pt.vectorDirect}(values));
    }

    /**
     * Returns the indices of values sorted from largest to smallest.
     *
     * @param values values.
     * @return sorted indices.
     */
    public static int[] sortDescendingIndex(final ${pt.boxed}[] values) {
        if (values == null) {
            return null;
        }

        final ${pt.primitive}[] vs = new ${pt.primitive}[values.length];
        for (int i = 0; i < values.length; i++) {
            vs[i] = isNull(values[i]) ? ${pt.null} : values[i];
        }

        return sortDescendingIndex(new ${pt.vectorDirect}(vs));
    }

    </#if>
    </#list>
}
