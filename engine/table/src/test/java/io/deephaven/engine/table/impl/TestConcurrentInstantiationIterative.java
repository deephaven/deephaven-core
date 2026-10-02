//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Category(OutOfBandTest.class)
public class TestConcurrentInstantiationIterative extends TestConcurrentInstantiationIterativeBase {
    @Test
    public void testIterative() {
        final List<Function<Table, Table>> transformations = new ArrayList<>();
        transformations.add(t -> t.updateView("i4=intCol * 4"));
        transformations.add(t -> t.where("boolCol"));
        transformations.add(t -> t.where("boolCol2"));
        transformations.add(t -> t.sortDescending("doubleCol"));
        transformations.add(Table::flatten);

        testIterative(transformations, 0, new MutableInt(50));
    }
}
