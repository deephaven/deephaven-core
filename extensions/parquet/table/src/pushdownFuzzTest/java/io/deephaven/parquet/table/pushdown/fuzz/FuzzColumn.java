//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.engine.table.impl.util.ColumnHolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One generated column of a fuzz case.
 *
 * <p>
 * A column carries two names. {@link #storageName()} is what the parquet file is written with; {@link #resultName()} is
 * what a filter must use after the case's renames have been applied. They differ exactly when the case renames this
 * column, which is the whole point of the rename dimension -- pushdown has to translate between them, and the in-memory
 * oracle has no such translation layer.
 */
public final class FuzzColumn {

    private final FuzzType type;
    private final String storageName;
    private final FuzzType.NullMode nullMode;
    private final FuzzType.Spread spread;
    private final List<Object> values;

    /** Assigned by the rename generator; defaults to the storage name. */
    private String resultName;

    /** Set when this column carries a parquet data index. */
    private boolean indexed;

    /** Set when this column is a key-value partitioning column. */
    private boolean partitioning;

    /** Set when the case sorts on this column. */
    private boolean sorted;

    FuzzColumn(
            final FuzzType type,
            final String storageName,
            final FuzzType.NullMode nullMode,
            final FuzzType.Spread spread,
            final List<Object> values) {
        this.type = type;
        this.storageName = storageName;
        this.resultName = storageName;
        this.nullMode = nullMode;
        this.spread = spread;
        this.values = values;
    }

    public FuzzType type() {
        return type;
    }

    public String storageName() {
        return storageName;
    }

    public String resultName() {
        return resultName;
    }

    void setResultName(final String resultName) {
        this.resultName = resultName;
    }

    public FuzzType.NullMode nullMode() {
        return nullMode;
    }

    public FuzzType.Spread spread() {
        return spread;
    }

    public List<Object> values() {
        return Collections.unmodifiableList(values);
    }

    public int size() {
        return values.size();
    }

    public boolean indexed() {
        return indexed;
    }

    void setIndexed(final boolean indexed) {
        this.indexed = indexed;
    }

    public boolean partitioning() {
        return partitioning;
    }

    void setPartitioning(final boolean partitioning) {
        this.partitioning = partitioning;
    }

    public boolean sorted() {
        return sorted;
    }

    void setSorted(final boolean sorted) {
        this.sorted = sorted;
    }

    /** Materialize this column, under its storage name. */
    public ColumnHolder<?> toColumnHolder() {
        return type.makeColumn(storageName, values);
    }

    /** Materialize the rows in {@code [from, to)} of this column, under its storage name. */
    public ColumnHolder<?> toColumnHolder(final int from, final int to) {
        return type.makeColumn(storageName, new ArrayList<>(values.subList(from, to)));
    }

    /** Distinct non-null value count, used to size dictionary limits so encoding can be mixed. */
    public int distinctNonNullCount() {
        return (int) values.stream().filter(java.util.Objects::nonNull).distinct().count();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(storageName).append(':').append(type.label());
        if (!storageName.equals(resultName)) {
            sb.append(" -> ").append(resultName);
        }
        sb.append(" nulls=").append(nullMode).append(" spread=").append(spread);
        if (indexed) {
            sb.append(" indexed");
        }
        if (partitioning) {
            sb.append(" partitioning");
        }
        if (sorted) {
            sb.append(" sorted");
        }
        return sb.toString();
    }
}
