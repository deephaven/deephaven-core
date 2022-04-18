
package io.deephaven.function;

import io.deephaven.vector.*;
import io.deephaven.function.comparators.NullNaNAwareComparator;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.Comparator;

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
        if (values.size() == 0) {
            return values.toArray();
        }

        final T[] vs = values.toArray();
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

        if (values.size() == 0) {
            return values.toArray();
        }

        final T[] vs = values.toArray();
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

    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber >

    //////////////////////////// ${pt.primitive} ////////////////////////////


    /**
     * Returns sorted values from smallest to largest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sort(final ${pt.dbArray} values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new ${pt.primitive}[0];
        }

        final ${pt.primitive}[] vs = Arrays.copyOf(values.toArray(), values.intSize("sort"));
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

        return sort(new ${pt.dbArrayDirect}(values));
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
     * Returns sorted values from largest to smallest.
     *
     * @param values values.
     * @return sorted values.
     */
    public static ${pt.primitive}[] sortDescending(final ${pt.dbArray} values) {
        if (values == null) {
            return null;
        }

        if (values.size() == 0) {
            return new ${pt.primitive}[0];
        }

        final ${pt.primitive}[] vs = Arrays.copyOf(values.toArray(), values.intSize("sortDescending"));
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

        return sortDescending(new ${pt.dbArrayDirect}(values));
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


    </#if>
    </#list>
}
