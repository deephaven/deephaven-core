/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.v2.LocalTableMap;
import io.deephaven.db.v2.TableMap;
import io.deephaven.util.annotations.ReferentialIntegrity;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A TableMap that filters and optionally transforms the keys of another TableMap.
 */
public class FilteredTableMap extends LocalTableMap {
    @ReferentialIntegrity
    private final KeyListener keyListener;

    /**
     * Create a filtered TableMap, using the identity transform.
     *
     * @param source the TableMap to filter
     * @param filter the predicate for filtering the keys
     */
    public FilteredTableMap(TableMap source, Predicate<Object> filter) {
        this(source, filter, x -> x);
    }

    /**
     * Create a filtered TableMap.
     *
     * @param source the TableMap to filter
     * @param filter the predicate for filtering the keys
     * @param keyTransformer a function that transforms the source's keys to our output's keys
     */
    public FilteredTableMap(TableMap source, Predicate<Object> filter, Function<Object, Object> keyTransformer) {
        super(null, (source instanceof LocalTableMap) ? ((LocalTableMap) source).getConstituentDefinition().orElse(null)
                : null);
        addParentReference(source);
        setDependency((NotificationQueue.Dependency) source);
        for (final Object key : source.getKeySet()) {
            if (filter.test(key)) {
                put(keyTransformer.apply(key), source.get(key));
            }
        }
        source.addKeyListener(keyListener = key -> {
            if (filter.test(key)) {
                final Object newKey = keyTransformer.apply(key);
                final Table oldTable = put(newKey, source.get(key));
                if (oldTable != null) {
                    throw new IllegalStateException("Can not replace a table in a FilteredTableMap, new key=" + newKey
                            + ", original key=" + key);
                }
            }
        });
    }
}
