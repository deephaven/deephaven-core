/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

public class SelectDistinctSpecImpl implements AggregationSpec {
    private static final AggregationMemoKey SELECT_DISTINCT_INSTANCE = new AggregationMemoKey() {};

    @Override
    public AggregationMemoKey getMemoKey() {
        return SELECT_DISTINCT_INSTANCE;
    }

    @Override
    public String toString() {
        return "SelectDistinctSpecImpl()";
    }
}
