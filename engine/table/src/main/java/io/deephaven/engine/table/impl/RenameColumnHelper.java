//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Pair;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RenameColumnHelper {
    /**
     * Create a lookup map from the provided pairs while validating that the pairs are valid (i.e. ensure source columns
     * exist, no duplicate source columns, no duplicate destination columns).
     */
    public static Map<String, String> createLookupAndValidate(final TableDefinition definition,
            final Collection<Pair> pairs) {
        Set<String> notFound = null;
        Set<String> duplicateSource = null;
        Set<String> duplicateDest = null;

        final Set<String> newNames = new HashSet<>();
        final Map<String, String> pairLookup = new LinkedHashMap<>();
        for (final Pair pair : pairs) {
            ColumnDefinition<?> cDef = definition.getColumn(pair.input().name());
            if (cDef == null) {
                (notFound == null ? notFound = new LinkedHashSet<>() : notFound)
                        .add(pair.input().name());
            }
            if (pairLookup.put(pair.input().name(), pair.output().name()) != null) {
                (duplicateSource == null ? duplicateSource = new LinkedHashSet<>(1) : duplicateSource)
                        .add(pair.input().name());
            }
            if (!newNames.add(pair.output().name())) {
                (duplicateDest == null ? duplicateDest = new LinkedHashSet<>(1) : duplicateDest)
                        .add(pair.output().name());
            }
        }

        // if we accumulated any errors, build one mega error message and throw it
        if (notFound != null || duplicateSource != null || duplicateDest != null) {
            throw new IllegalArgumentException(Stream.of(
                    notFound == null ? null : "Column(s) not found: " + String.join(", ", notFound),
                    duplicateSource == null ? null
                            : "Duplicate source column(s): " + String.join(", ", duplicateSource),
                    duplicateDest == null ? null
                            : "Duplicate destination column(s): " + String.join(", ", duplicateDest))
                    .filter(Objects::nonNull).collect(Collectors.joining("\n")));
        }

        // No errors, return the lookup map
        return pairLookup;
    }

    /**
     * Get the set of new column names from the provided pairs.
     */
    public static Set<String> getNewColumns(final Collection<Pair> pairs) {
        return pairs.stream().map(p -> p.output().name()).collect(Collectors.toSet());
    }

    /**
     * Get the set of column names that will be masked by a rename operation (i.e. table columns that are being replaced
     * by a different renamed column).
     */
    public static Set<String> getMaskedColumns(final TableDefinition definition, final Collection<Pair> pairs) {
        return pairs.stream().map(p -> p.output().name()).filter(n -> definition.getColumn(n) != null)
                .collect(Collectors.toSet());
    }
}
