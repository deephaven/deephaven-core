<#--
  Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
-->

package io.deephaven.function;

import io.deephaven.vector.*;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.Basic.isNull;

/**
 * Functions for performing binary searches.
 */
@SuppressWarnings("SameParameterValue")
public class BinSearch {

    //////////////////////////// Object ////////////////////////////


    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index of where the key would be inserted.
     */
    static public <T extends Comparable<? super T>> int binSearchIndex(T[] values, T key, BinSearchAlgo choiceWhenEquals) {
        if (values == null || key == null) {
            return QueryConstants.NULL_INT;
        }

        return binSearchIndex(new ObjectVectorDirect<>(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index of where the key would be inserted.
     */
    static public <T extends Comparable<? super T>> int binSearchIndex(ObjectVector<T> values, T key, BinSearchAlgo choiceWhenEquals) {
        int index = rawBinSearchIndex(values, key, choiceWhenEquals);
        if (index == QueryConstants.NULL_INT) {
            return index;
        }

        if (index < 0) {
            return -index - 1;
        } else {
            return index;
        }
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    static public <T extends Comparable<? super T>> int rawBinSearchIndex(T[] values, T key, BinSearchAlgo choiceWhenEquals) {
        if (values == null || key == null) {
            return QueryConstants.NULL_INT;
        }

        return rawBinSearchIndex(new ObjectVectorDirect<>(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    static public <T extends Comparable<? super T>> int rawBinSearchIndex(ObjectVector<T> values, T key, BinSearchAlgo choiceWhenEquals) {
        if (values == null || key == null) {
            return QueryConstants.NULL_INT;
        }

        if (choiceWhenEquals != BinSearchAlgo.BS_ANY) {
            return binarySearch0Modified(values, 0, LongSizedDataStructure.intSize("rawBinSearchIndex", values.size()), key, choiceWhenEquals == BinSearchAlgo.BS_HIGHEST);
        } else {
            return binarySearch0(values, 0, LongSizedDataStructure.intSize("rawBinSearchIndex", values.size()), key);
        }
    }

    static private <T extends Comparable<? super T>> int binarySearch0(ObjectVector<T> array, int fromIndex, int toIndex, T key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = array.get(mid);
            if (midVal == null) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            int cmp = key.compareTo(midVal);
            if (cmp > 0) {
                low = mid + 1;
            } else if (cmp < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);  // key not found.
    }

    static private <T extends Comparable<? super T>> int binarySearch0Modified(ObjectVector<T> array, int fromIndex, int toIndex, T key, boolean highestOrLowest) {
        if(array.isEmpty()){
            return -1;
        }

        int low = fromIndex;
        int high = toIndex - 1;

        final T lowVal = array.get(low);
        final T highVal = array.get(high);
        final boolean isNullLow = isNull(lowVal);
        final boolean isNullHigh = isNull(highVal);

        if (highestOrLowest) {
            if (high >= low && !isNullHigh && key.compareTo(highVal) == 0) {
                return high;
            }
        } else if (low <= high && !isNullLow && key.compareTo(lowVal) == 0) {
            return low;
        }

        if (isNullLow) {
            throw new RuntimeException("Can't have a null in the array!");
        }

        if (isNullHigh) {
            throw new RuntimeException("Can't have a null in the array!");
        }

        while (low <= high) {
            int mid = highestOrLowest ? (low + high + 1) >>> 1 : (low + high) >>> 1;
            T midVal = array.get(mid);

            if (isNull(midVal)) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            int cmp = key.compareTo(midVal);
            if (cmp > 0) {
                low = mid + 1;
                if (low <= high) {
                    if (!highestOrLowest && key.compareTo(array.get(low)) == 0) {
                        return low;
                    }
                }
            } else if (cmp < 0) {
                high = mid - 1;
                if (high >= low) {
                    if (highestOrLowest && key.compareTo(array.get(high)) == 0) {
                        return high;
                    }
                }
            } else {
                if (highestOrLowest) {
                    low = mid;
                } else {
                    high = mid;
                }
            }
        }

        return -(low + 1);  // key not found.
    }


    <#list primitiveTypes as pt>
    <#if pt.valueType.isNumber || pt.valueType.isChar >

    //////////////////////////// ${pt.primitive} ////////////////////////////


    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index immediately before where the key would be inserted.
     */
    public static int binSearchIndex(${pt.primitive}[] values, ${pt.primitive} key, BinSearchAlgo choiceWhenEquals) {
        if (values == null) {
            return NULL_INT;
        }

        return binSearchIndex(new ${pt.vectorDirect}(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, the index of where the key would be inserted.
     */
    public static int binSearchIndex(${pt.vector} values, ${pt.primitive} key, BinSearchAlgo choiceWhenEquals) {
        int index = rawBinSearchIndex(values, key, choiceWhenEquals);
        if (index == NULL_INT) {
            return index;
        }

        if (index < 0) {
            return -index - 1;
        } else {
            return index;
        }
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    public static int rawBinSearchIndex(${pt.primitive}[] values, ${pt.primitive} key, BinSearchAlgo choiceWhenEquals) {
        if (values == null) {
            return NULL_INT;
        }

        return rawBinSearchIndex(new ${pt.vectorDirect}(values), key, choiceWhenEquals);
    }

    /**
     * Performs a binary search to find a key.
     *
     * @param values sorted values to search.  Null values are not supported.
     * @param key key to search for.  Null keys are not supported.
     * @param choiceWhenEquals algorithm used to resolve ties when performing a binary search.
     * @return index of the search key, if it is contained in the array; otherwise, {@code (-(insertion point) - 1)}.
     */
    public static int rawBinSearchIndex(${pt.vector} values, ${pt.primitive} key, BinSearchAlgo choiceWhenEquals) {
        if (values == null || key == ${pt.null}) {
            return NULL_INT;
        }

        if (choiceWhenEquals != BinSearchAlgo.BS_ANY) {
            return binarySearch0Modified(values, 0, values.intSize("rawBinSearchIndex"), key, choiceWhenEquals == BinSearchAlgo.BS_HIGHEST);
        } else {
            return binarySearch0(values, 0, values.intSize("rawBinSearchIndex"), key);
        }
    }

    static private int binarySearch0(${pt.vector} array, int fromIndex, int toIndex, ${pt.primitive} key) {
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            ${pt.primitive} midVal = array.get(mid);
            if (midVal == ${pt.null}) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            if (midVal < key)
                low = mid + 1;
            else if (midVal > key)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found.
    }

    static private int binarySearch0Modified(${pt.vector} array, int fromIndex, int toIndex, ${pt.primitive} key, boolean highestOrLowest) {
        if(array.isEmpty()){
            return -1;
        }

        int low = fromIndex;
        int high = toIndex - 1;

        final ${pt.primitive} lowVal = array.get(low);
        final ${pt.primitive} highVal = array.get(high);

        if (highestOrLowest) {
            if (high >= low && key == highVal && highVal != ${pt.null}) {
                return high;
            }
        } else if (low <= high && key == lowVal && lowVal != ${pt.null}) {
            return low;
        }

        if (lowVal == ${pt.null}) {
            throw new RuntimeException("Can't have a null in the array!");
        }

        if (highVal == ${pt.null}) {
            throw new RuntimeException("Can't have a null in the array!");
        }

        while (low <= high) {
            int mid = highestOrLowest ? (low + high + 1) >>> 1 : (low + high) >>> 1;
            ${pt.primitive} midVal = array.get(mid);

            if (midVal == ${pt.null}) {
                throw new RuntimeException("Can't have a null in the array!");
            }

            if (key > midVal) {
                low = mid + 1;
                if (low <= high) {
                    if (!highestOrLowest && key == array.get(low)) {
                        return low;
                    }
                }
            } else if (key < midVal) {
                high = mid - 1;
                if (high >= low) {
                    if (highestOrLowest && key == array.get(high)) {
                        return high;
                    }
                }
            } else {
                if (highestOrLowest) {
                    low = mid;
                } else {
                    high = mid;
                }
            }
        }

        return -(low + 1);  // key not found.
    }

    </#if>
    </#list>
}
