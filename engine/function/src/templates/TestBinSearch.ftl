<#--
  Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
-->

package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.*;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.function.BinSearch.*;

/**
 * Test BinSearch.
 */
@SuppressWarnings({"RedundantCast", "RedundantArrayCreation"})
public class TestBinSearch extends BaseArrayTestCase {

    //////////////////////////// Object ////////////////////////////

    public void testGenericBinSearchIndex() {
        Short[] data = {1,3,4};
        assertEquals(NULL_INT, binSearchIndex((ObjectVector<Short>)null, (short) 0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)1, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)2, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)3, BinSearchAlgo.BS_ANY));
        assertEquals(2, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)4, BinSearchAlgo.BS_ANY));
        assertEquals(3, binSearchIndex(new ObjectVectorDirect<Short>(data), (short)5, BinSearchAlgo.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((Short[])null, (short) 0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(data, (short)0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(data, (short)1, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(data, (short)2, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(data, (short)3, BinSearchAlgo.BS_ANY));
        assertEquals(2, binSearchIndex(data, (short)4, BinSearchAlgo.BS_ANY));
        assertEquals(3, binSearchIndex(data, (short)5, BinSearchAlgo.BS_ANY));
    }

    public void testGenericRawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((ObjectVector<Short>)null, (short) 0, BinSearchAlgo.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((ObjectVector<Short>)null, (short) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((ObjectVector<Short>)null, (short) 0, BinSearchAlgo.BS_LOWEST));

        Short[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(empty), (short) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(empty), (short) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(empty), (short) 0, BinSearchAlgo.BS_LOWEST));

        Short[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 12, BinSearchAlgo.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 12, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 12, BinSearchAlgo.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 11, BinSearchAlgo.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 11, BinSearchAlgo.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(one), (short) 11, BinSearchAlgo.BS_LOWEST));


        Short[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((ObjectVector<Short>)null, (short) 0, null);

        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 26, BinSearchAlgo.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 26, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 26, BinSearchAlgo.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 1, BinSearchAlgo.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 1, BinSearchAlgo.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 1, BinSearchAlgo.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 2, BinSearchAlgo.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 2, BinSearchAlgo.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 3, BinSearchAlgo.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 3, BinSearchAlgo.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 4, BinSearchAlgo.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 4, BinSearchAlgo.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 5, BinSearchAlgo.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 5, BinSearchAlgo.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 7, BinSearchAlgo.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 7, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 7, BinSearchAlgo.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 10, BinSearchAlgo.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 10, BinSearchAlgo.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 11, BinSearchAlgo.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 11, BinSearchAlgo.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 15, BinSearchAlgo.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 15, BinSearchAlgo.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 15, BinSearchAlgo.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 25, BinSearchAlgo.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new ObjectVectorDirect<Short>(v), (short) 25, BinSearchAlgo.BS_LOWEST));

        rawBinSearchIndex((Short[])null, (short) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (short) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (short) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (short) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (short) 26, BinSearchAlgo.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (short) 26, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (short) 26, BinSearchAlgo.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (short) 1, BinSearchAlgo.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (short) 1, BinSearchAlgo.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (short) 1, BinSearchAlgo.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (short) 2, BinSearchAlgo.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (short) 2, BinSearchAlgo.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (short) 3, BinSearchAlgo.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (short) 3, BinSearchAlgo.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (short) 4, BinSearchAlgo.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (short) 4, BinSearchAlgo.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (short) 5, BinSearchAlgo.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (short) 5, BinSearchAlgo.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (short) 7, BinSearchAlgo.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (short) 7, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (short) 7, BinSearchAlgo.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (short) 10, BinSearchAlgo.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (short) 10, BinSearchAlgo.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (short) 11, BinSearchAlgo.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (short) 11, BinSearchAlgo.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (short) 15, BinSearchAlgo.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (short) 15, BinSearchAlgo.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (short) 15, BinSearchAlgo.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (short) 25, BinSearchAlgo.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (short) 25, BinSearchAlgo.BS_LOWEST));
    }


    <#list primitiveTypes as pt>

    public void test${pt.boxed}BinSearchIndex() {
        assertEquals(NULL_INT, binSearchIndex((${pt.primitive}[]) null, (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(new ${pt.primitive}[]{1,3,4}, (${pt.primitive})0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(new ${pt.primitive}[]{1,3,4}, (${pt.primitive})1, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(new ${pt.primitive}[]{1,3,4}, (${pt.primitive})2, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(new ${pt.primitive}[]{1,3,4}, (${pt.primitive})3, BinSearchAlgo.BS_ANY));
        assertEquals(2, binSearchIndex(new ${pt.primitive}[]{1,3,4}, (${pt.primitive})4, BinSearchAlgo.BS_ANY));
        assertEquals(3, binSearchIndex(new ${pt.primitive}[]{1,3,4}, (${pt.primitive})5, BinSearchAlgo.BS_ANY));

        assertEquals(NULL_INT, binSearchIndex((${pt.vector}) null, (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,3,4}), (${pt.primitive})0, BinSearchAlgo.BS_ANY));
        assertEquals(0, binSearchIndex(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,3,4}), (${pt.primitive})1, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,3,4}), (${pt.primitive})2, BinSearchAlgo.BS_ANY));
        assertEquals(1, binSearchIndex(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,3,4}), (${pt.primitive})3, BinSearchAlgo.BS_ANY));
        assertEquals(2, binSearchIndex(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,3,4}), (${pt.primitive})4, BinSearchAlgo.BS_ANY));
        assertEquals(3, binSearchIndex(new ${pt.vectorDirect}(new ${pt.primitive}[]{1,3,4}), (${pt.primitive})5, BinSearchAlgo.BS_ANY));
    }

    public void test${pt.boxed}RawBinSearchIndex() {
        assertEquals(NULL_INT, rawBinSearchIndex((${pt.vector})null, (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((${pt.vector})null, (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((${pt.vector})null, (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        ${pt.primitive}[] empty = {};
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(empty), (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(empty), (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(empty), (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        ${pt.primitive}[] one = {11};
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 12, BinSearchAlgo.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 12, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 12, BinSearchAlgo.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 11, BinSearchAlgo.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 11, BinSearchAlgo.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ${pt.vectorDirect}(one), (${pt.primitive}) 11, BinSearchAlgo.BS_LOWEST));


        ${pt.primitive}[] v = {1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 15, 20, 20, 25, 25};

        rawBinSearchIndex((${pt.vector})null, (${pt.primitive}) 0, null);

        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 26, BinSearchAlgo.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 26, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 26, BinSearchAlgo.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 1, BinSearchAlgo.BS_ANY));
        assertEquals(0, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 1, BinSearchAlgo.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 1, BinSearchAlgo.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 2, BinSearchAlgo.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 2, BinSearchAlgo.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 3, BinSearchAlgo.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 3, BinSearchAlgo.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 4, BinSearchAlgo.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 4, BinSearchAlgo.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 5, BinSearchAlgo.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 5, BinSearchAlgo.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 7, BinSearchAlgo.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 7, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 7, BinSearchAlgo.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 10, BinSearchAlgo.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 10, BinSearchAlgo.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 11, BinSearchAlgo.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 11, BinSearchAlgo.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 15, BinSearchAlgo.BS_ANY));
        assertEquals(25, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 15, BinSearchAlgo.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 15, BinSearchAlgo.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 25, BinSearchAlgo.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(new ${pt.vectorDirect}(v), (${pt.primitive}) 25, BinSearchAlgo.BS_LOWEST));

        /////

        assertEquals(NULL_INT, rawBinSearchIndex((${pt.primitive}[]) null, (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(NULL_INT, rawBinSearchIndex((${pt.primitive}[])null, (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(NULL_INT, rawBinSearchIndex((${pt.primitive}[])null, (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(empty, (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(empty, (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(empty, (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-1, rawBinSearchIndex(one, (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(one, (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(one, (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-2, rawBinSearchIndex(one, (${pt.primitive}) 12, BinSearchAlgo.BS_ANY));
        assertEquals(-2, rawBinSearchIndex(one, (${pt.primitive}) 12, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-2, rawBinSearchIndex(one, (${pt.primitive}) 12, BinSearchAlgo.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(one, (${pt.primitive}) 11, BinSearchAlgo.BS_ANY));
        assertEquals(0, rawBinSearchIndex(one, (${pt.primitive}) 11, BinSearchAlgo.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(one, (${pt.primitive}) 11, BinSearchAlgo.BS_LOWEST));


        rawBinSearchIndex((${pt.primitive}[])null, (${pt.primitive}) 0, null);

        assertEquals(-1, rawBinSearchIndex(v, (${pt.primitive}) 0, BinSearchAlgo.BS_ANY));
        assertEquals(-1, rawBinSearchIndex(v, (${pt.primitive}) 0, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-1, rawBinSearchIndex(v, (${pt.primitive}) 0, BinSearchAlgo.BS_LOWEST));

        assertEquals(-v.length - 1, rawBinSearchIndex(v, (${pt.primitive}) 26, BinSearchAlgo.BS_ANY));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (${pt.primitive}) 26, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-v.length - 1, rawBinSearchIndex(v, (${pt.primitive}) 26, BinSearchAlgo.BS_LOWEST));

        assertEquals(0, rawBinSearchIndex(v, (${pt.primitive}) 1, BinSearchAlgo.BS_ANY));
        assertEquals(0, rawBinSearchIndex(v, (${pt.primitive}) 1, BinSearchAlgo.BS_HIGHEST));
        assertEquals(0, rawBinSearchIndex(v, (${pt.primitive}) 1, BinSearchAlgo.BS_LOWEST));

        assertEquals(2, rawBinSearchIndex(v, (${pt.primitive}) 2, BinSearchAlgo.BS_HIGHEST));
        assertEquals(1, rawBinSearchIndex(v, (${pt.primitive}) 2, BinSearchAlgo.BS_LOWEST));

        assertEquals(5, rawBinSearchIndex(v, (${pt.primitive}) 3, BinSearchAlgo.BS_HIGHEST));
        assertEquals(3, rawBinSearchIndex(v, (${pt.primitive}) 3, BinSearchAlgo.BS_LOWEST));

        assertEquals(9, rawBinSearchIndex(v, (${pt.primitive}) 4, BinSearchAlgo.BS_HIGHEST));
        assertEquals(6, rawBinSearchIndex(v, (${pt.primitive}) 4, BinSearchAlgo.BS_LOWEST));

        assertEquals(14, rawBinSearchIndex(v, (${pt.primitive}) 5, BinSearchAlgo.BS_HIGHEST));
        assertEquals(10, rawBinSearchIndex(v, (${pt.primitive}) 5, BinSearchAlgo.BS_LOWEST));

        assertEquals(-16, rawBinSearchIndex(v, (${pt.primitive}) 7, BinSearchAlgo.BS_ANY));
        assertEquals(-16, rawBinSearchIndex(v, (${pt.primitive}) 7, BinSearchAlgo.BS_HIGHEST));
        assertEquals(-16, rawBinSearchIndex(v, (${pt.primitive}) 7, BinSearchAlgo.BS_LOWEST));

        assertEquals(19, rawBinSearchIndex(v, (${pt.primitive}) 10, BinSearchAlgo.BS_HIGHEST));
        assertEquals(15, rawBinSearchIndex(v, (${pt.primitive}) 10, BinSearchAlgo.BS_LOWEST));

        assertEquals(24, rawBinSearchIndex(v, (${pt.primitive}) 11, BinSearchAlgo.BS_HIGHEST));
        assertEquals(20, rawBinSearchIndex(v, (${pt.primitive}) 11, BinSearchAlgo.BS_LOWEST));

        assertEquals(25, rawBinSearchIndex(v, (${pt.primitive}) 15, BinSearchAlgo.BS_ANY));
        assertEquals(25, rawBinSearchIndex(v, (${pt.primitive}) 15, BinSearchAlgo.BS_HIGHEST));
        assertEquals(25, rawBinSearchIndex(v, (${pt.primitive}) 15, BinSearchAlgo.BS_LOWEST));

        assertEquals(29, rawBinSearchIndex(v, (${pt.primitive}) 25, BinSearchAlgo.BS_HIGHEST));
        assertEquals(28, rawBinSearchIndex(v, (${pt.primitive}) 25, BinSearchAlgo.BS_LOWEST));
    }

    </#list>
}
