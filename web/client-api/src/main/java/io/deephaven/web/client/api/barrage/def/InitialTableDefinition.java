/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.barrage.def;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A table definition constructed when using the fetch command; also includes the table id and size, which are not
 * normally part of a table definition (as they will change when the table evolves)
 */
public class InitialTableDefinition {

    private ColumnDefinition[] columns;
    private TableAttributesDefinition attributes;

    public ColumnDefinition[] getColumns() {
        return columns;
    }

    public InitialTableDefinition setColumns(final ColumnDefinition[] columns) {
        this.columns = columns;
        return this;
    }

    public TableAttributesDefinition getAttributes() {
        return attributes;
    }

    public InitialTableDefinition setAttributes(final TableAttributesDefinition attributes) {
        this.attributes = attributes;
        return this;
    }

    public Map<Boolean, Map<String, ColumnDefinition>> getColumnsByName() {
        return Arrays.stream(columns)
                .collect(Collectors.partitioningBy(ColumnDefinition::isRollupConstituentNodeColumn, columnCollector(false)));
    }

    private static Collector<? super ColumnDefinition, ?, Map<String, ColumnDefinition>> columnCollector(
            boolean ordered) {
        return Collectors.toMap(ColumnDefinition::getName, Function.identity(), assertNoDupes(),
                ordered ? LinkedHashMap::new : HashMap::new);
    }

    private static <T> BinaryOperator<T> assertNoDupes() {
        return (u, v) -> {
            assert u == v : "Duplicates found for " + u + " and " + v;
            return u;
        };
    }

}
