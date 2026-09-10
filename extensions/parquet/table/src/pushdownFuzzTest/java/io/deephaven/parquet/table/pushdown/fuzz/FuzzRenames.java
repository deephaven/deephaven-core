//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.engine.table.Table;
import io.deephaven.parquet.table.ParquetInstructions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Column-rename generation -- the highest-value axis of the bench.
 *
 * <p>
 * Pushdown carries its own name-translation layer, separate from the one the rest of {@code where()} uses: a filter
 * names a column in <em>result</em> space, while a statistics/dictionary/data-index handler needs the name in
 * <em>storage</em> space. Three independent pieces of code perform that translation --
 * {@code PushdownPredicateManager.computeRenameMap}, {@code RegionedPushdownFilterContextImpl
 * .filterColumnToManagerColumnName}, and {@code ParquetTableLocation.resolveColumns}. A bug in any of them reads
 * metadata for the <em>wrong column</em>, which yields wrong rows silently rather than an exception. The in-memory
 * oracle has no such layer, so it remains a valid witness.
 *
 * <p>
 * The two existing hand-written tests both rename every column with a uniform {@code _renamed} suffix, which is the
 * shape <em>least</em> likely to expose a mapping bug because every wrong answer stays self-consistent. The shapes
 * below are chosen to break that symmetry: partial renames, permutations, chains, and collisions.
 */
public final class FuzzRenames {

    /** Where the rename happens. These compose: {@link #BOTH} makes storage, manager and filter names all differ. */
    public enum Mechanism {
        /** Control. */
        NONE,
        /** {@code ParquetInstructions.addColumnNameMapping} -- renames below the table. */
        INSTRUCTION,
        /** {@code Table.renameColumns} after the read -- renames above the location. */
        TABLE,
        /** Both, stacked, so three different name spaces are in play. */
        BOTH
    }

    /** The interesting part: how the mapping is shaped, not merely whether one exists. */
    public enum Shape {
        NONE,
        /** Every column gets the same suffix -- what the existing tests do. Included as a baseline. */
        ALL_SUFFIX,
        /** Only some columns are renamed, so renamed and non-renamed coexist in one filter. */
        PARTIAL,
        /**
         * Names swap: {@code renameColumns("A=B", "B=A")}. Every name stays valid, so a translation applied in the
         * wrong direction still resolves -- to the wrong column.
         */
        PERMUTATION,
        /** Renamed, then renamed again. */
        CHAINED,
        /** A column is renamed to a name another column used to have. */
        COLLISION,
        /** Case-only differences and prefix relationships. */
        ADVERSARIAL
    }

    private final Mechanism mechanism;
    private final Shape shape;

    /** Parquet column name -> table column name, for {@code addColumnNameMapping}. */
    private final Map<String, String> instructionMappings = new LinkedHashMap<>();

    /** Successive {@code renameColumns} rounds, each a list of {@code "new=old"} pairs. */
    private final List<List<String>> tableRenameRounds = new ArrayList<>();

    /** Alias name -> source column result name, materialized with {@code updateView}. */
    private final Map<String, String> duplicates = new LinkedHashMap<>();

    private FuzzRenames(final Mechanism mechanism, final Shape shape) {
        this.mechanism = mechanism;
        this.shape = shape;
    }

    /** The no-rename control. */
    public static FuzzRenames none() {
        return new FuzzRenames(Mechanism.NONE, Shape.NONE);
    }

    /**
     * Draw a rename spec and apply it to {@code columns}, updating each column's result name.
     *
     * @param random the source of randomness
     * @param columns the case's columns, whose result names are updated in place
     * @param renameProbability the chance of applying any rename at all
     */
    public static FuzzRenames generate(
            final Random random,
            final List<FuzzColumn> columns,
            final double renameProbability) {
        if (columns.isEmpty() || random.nextDouble() >= renameProbability) {
            return none();
        }

        final Mechanism mechanism = pickMechanism(random);
        final Shape shape = pickShape(random, columns.size());
        final FuzzRenames renames = new FuzzRenames(mechanism, shape);
        renames.build(random, columns);

        // Column duplication is orthogonal to the rename mechanism, and is its own hazard:
        // computeRenameMap keys on an IdentityHashMap<ColumnSource<?>, String>, so when two result
        // columns share one ColumnSource only one of the names survives in the reverse lookup.
        if (random.nextInt(5) == 0) {
            final FuzzColumn source = columns.get(random.nextInt(columns.size()));
            renames.duplicates.put(uniqueName("dup", source.resultName(), columns, renames), source.resultName());
        }
        return renames;
    }

    private static Mechanism pickMechanism(final Random random) {
        final int roll = random.nextInt(10);
        if (roll < 4) {
            return Mechanism.INSTRUCTION;
        }
        if (roll < 8) {
            return Mechanism.TABLE;
        }
        return Mechanism.BOTH;
    }

    private static Shape pickShape(final Random random, final int columnCount) {
        final List<Shape> candidates = new ArrayList<>();
        candidates.add(Shape.ALL_SUFFIX);
        candidates.add(Shape.PARTIAL);
        candidates.add(Shape.CHAINED);
        candidates.add(Shape.ADVERSARIAL);
        if (columnCount >= 2) {
            // These need at least two columns to be meaningful.
            candidates.add(Shape.PERMUTATION);
            candidates.add(Shape.PERMUTATION);
            candidates.add(Shape.COLLISION);
        }
        return candidates.get(random.nextInt(candidates.size()));
    }

    /**
     * Compute the storage -> result mapping for the chosen shape, then route it through the chosen mechanism.
     *
     * <p>
     * The mapping is computed once, in result space, and then split: {@link Mechanism#INSTRUCTION} performs it all at
     * read time, {@link Mechanism#TABLE} all after the read, and {@link Mechanism#BOTH} performs half at read time
     * (into an intermediate name) and half afterwards, so that storage, manager and filter names all differ.
     */
    private void build(final Random random, final List<FuzzColumn> columns) {
        final Map<String, String> storageToResult = new LinkedHashMap<>();
        switch (shape) {
            case ALL_SUFFIX:
                for (final FuzzColumn column : columns) {
                    storageToResult.put(column.storageName(), column.storageName() + "_renamed");
                }
                break;
            case PARTIAL:
                for (final FuzzColumn column : columns) {
                    if (random.nextBoolean()) {
                        storageToResult.put(column.storageName(), column.storageName() + "_r");
                    }
                }
                if (storageToResult.isEmpty()) {
                    // Guarantee at least one rename, or this degenerates into the control.
                    final FuzzColumn column = columns.get(random.nextInt(columns.size()));
                    storageToResult.put(column.storageName(), column.storageName() + "_r");
                }
                break;
            case PERMUTATION: {
                // Rotate the names of a randomly chosen subset, so every name remains valid but
                // belongs to a different column.
                final List<FuzzColumn> subset = new ArrayList<>(columns);
                Collections.shuffle(subset, random);
                final int rotate = 2 + random.nextInt(Math.max(1, subset.size() - 1));
                final List<FuzzColumn> cycle = subset.subList(0, Math.min(rotate, subset.size()));
                for (int ii = 0; ii < cycle.size(); ++ii) {
                    final FuzzColumn from = cycle.get(ii);
                    final FuzzColumn to = cycle.get((ii + 1) % cycle.size());
                    storageToResult.put(from.storageName(), to.storageName());
                }
                break;
            }
            case COLLISION: {
                // Rename one column to a name another column is giving up in the same round.
                final List<FuzzColumn> subset = new ArrayList<>(columns);
                Collections.shuffle(subset, random);
                final FuzzColumn giver = subset.get(0);
                final FuzzColumn taker = subset.get(1);
                storageToResult.put(giver.storageName(), giver.storageName() + "_moved");
                storageToResult.put(taker.storageName(), giver.storageName());
                break;
            }
            case ADVERSARIAL:
                for (final FuzzColumn column : columns) {
                    final String base = column.storageName();
                    final int roll = random.nextInt(3);
                    if (roll == 0) {
                        // Case-only difference.
                        storageToResult.put(base, swapFirstCase(base));
                    } else if (roll == 1) {
                        // A prefix relationship with the original.
                        storageToResult.put(base, base + "2");
                    }
                    // roll == 2: leave this one alone, so shapes mix.
                }
                if (storageToResult.isEmpty()) {
                    final FuzzColumn column = columns.get(random.nextInt(columns.size()));
                    storageToResult.put(column.storageName(), swapFirstCase(column.storageName()));
                }
                break;
            case NONE:
            default:
                return;
        }

        switch (mechanism) {
            case INSTRUCTION:
                instructionMappings.putAll(storageToResult);
                break;
            case TABLE:
                tableRenameRounds.add(toRenamePairs(storageToResult));
                break;
            case BOTH: {
                // Storage -> intermediate at read time, intermediate -> final afterwards.
                final Map<String, String> intermediate = new LinkedHashMap<>();
                final Map<String, String> finalStep = new LinkedHashMap<>();
                storageToResult.forEach((storage, result) -> {
                    final String mid = "m_" + result;
                    intermediate.put(storage, mid);
                    finalStep.put(mid, result);
                });
                instructionMappings.putAll(intermediate);
                tableRenameRounds.add(toRenamePairs(finalStep));
                break;
            }
            case NONE:
            default:
                break;
        }

        if (shape == Shape.CHAINED) {
            // A second round on top of whatever the first produced.
            final Map<String, String> second = new LinkedHashMap<>();
            for (final Map.Entry<String, String> entry : storageToResult.entrySet()) {
                second.put(entry.getValue(), entry.getValue() + "_again");
            }
            tableRenameRounds.add(toRenamePairs(second));
            second.forEach((from, to) -> storageToResult.replaceAll((k, v) -> v.equals(from) ? to : v));
        }

        // Publish the final result names onto the columns.
        for (final FuzzColumn column : columns) {
            final String result = storageToResult.get(column.storageName());
            if (result != null) {
                column.setResultName(result);
            }
        }
    }

    private static List<String> toRenamePairs(final Map<String, String> fromTo) {
        final List<String> pairs = new ArrayList<>(fromTo.size());
        fromTo.forEach((from, to) -> pairs.add(to + "=" + from));
        return pairs;
    }

    private static String swapFirstCase(final String name) {
        final char first = name.charAt(0);
        final char swapped = Character.isUpperCase(first)
                ? Character.toLowerCase(first)
                : Character.toUpperCase(first);
        return swapped + name.substring(1);
    }

    private static String uniqueName(
            final String prefix,
            final String hint,
            final List<FuzzColumn> columns,
            final FuzzRenames renames) {
        for (int suffix = 0;; ++suffix) {
            final String candidate = prefix + suffix + "_" + hint;
            boolean clash = renames.duplicates.containsKey(candidate);
            for (final FuzzColumn column : columns) {
                clash |= candidate.equals(column.resultName()) || candidate.equals(column.storageName());
            }
            if (!clash) {
                return candidate;
            }
        }
    }

    // -------------------------------------------------------------------------------------------
    // Application
    // -------------------------------------------------------------------------------------------

    /** Add this spec's {@code addColumnNameMapping} entries to a read-instructions builder. */
    public ParquetInstructions.Builder applyInstructionMappings(final ParquetInstructions.Builder builder) {
        instructionMappings.forEach(builder::addColumnNameMapping);
        return builder;
    }

    /** Whether any read-time mapping is in play (the harness must then read with instructions). */
    public boolean hasInstructionMappings() {
        return !instructionMappings.isEmpty();
    }

    /** Apply the post-read {@code renameColumns} rounds and any column duplication. */
    public Table applyTableRenames(Table table) {
        for (final List<String> round : tableRenameRounds) {
            if (!round.isEmpty()) {
                table = table.renameColumns(round.toArray(new String[0]));
            }
        }
        if (!duplicates.isEmpty()) {
            final List<String> aliases = new ArrayList<>(duplicates.size());
            duplicates.forEach((alias, source) -> aliases.add(alias + " = " + source));
            // updateView, not update: the alias must share the source's ColumnSource, which is what
            // makes the IdentityHashMap in computeRenameMap ambiguous.
            table = table.updateView(aliases.toArray(new String[0]));
        }
        return table;
    }

    /** Alias name -> source result name, for filter generation over duplicated columns. */
    public Map<String, String> duplicates() {
        return Collections.unmodifiableMap(duplicates);
    }

    public Mechanism mechanism() {
        return mechanism;
    }

    public Shape shape() {
        return shape;
    }

    @Override
    public String toString() {
        if (mechanism == Mechanism.NONE && duplicates.isEmpty()) {
            return "renames=none";
        }
        final StringBuilder sb = new StringBuilder("renames=").append(mechanism).append('/').append(shape);
        if (!instructionMappings.isEmpty()) {
            sb.append(" instruction=").append(instructionMappings);
        }
        if (!tableRenameRounds.isEmpty()) {
            sb.append(" table=").append(tableRenameRounds);
        }
        if (!duplicates.isEmpty()) {
            sb.append(" duplicates=").append(duplicates);
        }
        return sb.toString();
    }
}
