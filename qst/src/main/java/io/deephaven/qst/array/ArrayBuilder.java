//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.array;

public interface ArrayBuilder<TYPE, ARRAY extends Array<TYPE>, SELF extends ArrayBuilder<TYPE, ARRAY, SELF>> {

    SELF add(TYPE item);

    SELF add(TYPE... items);

    SELF add(Iterable<TYPE> items);

    ARRAY build();
}
