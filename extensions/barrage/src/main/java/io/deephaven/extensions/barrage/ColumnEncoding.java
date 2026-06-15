//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

/**
 * Arrow encoding to apply to a column when generating a Barrage schema. Auto-detected per column by
 * {@link io.deephaven.extensions.barrage.util.BarrageUtil#schemaFromTable} when structural guarantees make a particular
 * encoding clearly beneficial, or specified explicitly via
 * {@link io.deephaven.engine.table.Table#BARRAGE_SCHEMA_ATTRIBUTE}.
 */
public enum ColumnEncoding {
    RUN_END_ENCODED, DICTIONARY_ENCODED
}
