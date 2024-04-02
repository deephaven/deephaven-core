//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public interface JoinableTable {
    @JsIgnore
    ClientTableState state();

    @JsMethod
    Promise<JsTable> freeze();

    @JsMethod
    Promise<JsTable> snapshot(JsTable baseTable, @JsOptional Boolean doInitialSnapshot,
            @JsOptional String[] stampColumns);

    @JsMethod
    @Deprecated
    Promise<JsTable> join(Object joinType, JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd, @JsOptional Object asOfMatchRule);

    @JsMethod
    Promise<JsTable> asOfJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd, @JsOptional String asOfMatchRule);

    @JsMethod
    Promise<JsTable> crossJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd, @JsOptional Double reserve_bits);

    @JsMethod
    Promise<JsTable> exactJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd);

    @JsMethod
    Promise<JsTable> naturalJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd);
}
