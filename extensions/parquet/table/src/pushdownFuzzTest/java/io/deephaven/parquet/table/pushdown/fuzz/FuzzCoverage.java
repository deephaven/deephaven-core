//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Tallies what a run actually exercised.
 *
 * <p>
 * Selection is pure random, so this does not steer generation -- it exists so coverage is <em>checkable from the
 * output</em> rather than assumed from the design. After a long run every eligible cell should be non-zero; a cell
 * still at zero after thousands of random draws means an eligibility flag or a generator is wrong, which is a bench bug
 * rather than a pushdown finding.
 */
public final class FuzzCoverage {

    private final Map<String, Integer> types = new TreeMap<>();
    private final Map<String, Integer> layouts = new TreeMap<>();
    private final Map<String, Integer> sortModes = new TreeMap<>();
    private final Map<String, Integer> dictionaryModes = new TreeMap<>();
    private final Map<String, Integer> renameMechanisms = new TreeMap<>();
    private final Map<String, Integer> renameShapes = new TreeMap<>();
    private final Map<String, Integer> sizes = new TreeMap<>();
    private final Map<String, Integer> roles = new TreeMap<>();
    private final Map<String, Integer> typeRoleSort = new TreeMap<>();

    /** Record one case. */
    public void record(final FuzzCase fuzzCase) {
        for (final FuzzColumn column : fuzzCase.columns()) {
            bump(types, column.type().label());
            if (column.sorted()) {
                bump(roles, "sorted:" + column.type().label());
                bump(typeRoleSort, column.type().label() + "/sorted/"
                        + fuzzCase.layout().sortOrder() + "/" + fuzzCase.layout().sortMode());
            }
            if (column.partitioning()) {
                bump(roles, "partitioning:" + column.type().label());
            }
            if (column.indexed()) {
                bump(roles, "indexed:" + column.type().label());
            }
        }
        bump(layouts, fuzzCase.layout().fileLayout().name());
        bump(sortModes, fuzzCase.layout().sortMode().name());
        bump(dictionaryModes, fuzzCase.layout().dictionaryMode().name());
        bump(renameMechanisms, fuzzCase.renames().mechanism().name());
        bump(renameShapes, fuzzCase.renames().shape().name());
        bump(sizes, String.format("%07d", fuzzCase.tableSize()));
    }

    private static void bump(final Map<String, Integer> counts, final String key) {
        counts.merge(key, 1, Integer::sum);
    }

    /** Render the report, flagging any dimension value that never came up. */
    public String report(final int casesRun) {
        final StringBuilder sb = new StringBuilder();
        sb.append("// ---- coverage over ").append(casesRun).append(" case(s) ----\n");
        appendSection(sb, "data types", types, expectedTypes());
        appendSection(sb, "file layouts", layouts, expectedNames(FuzzLayout.FileLayout.values()));
        appendSection(sb, "sort modes", sortModes, expectedNames(FuzzLayout.SortMode.values()));
        appendSection(sb, "dictionary modes", dictionaryModes,
                expectedNames(FuzzLayout.DictionaryMode.values()));
        appendSection(sb, "rename mechanisms", renameMechanisms,
                expectedNames(FuzzRenames.Mechanism.values()));
        appendSection(sb, "rename shapes", renameShapes, expectedNames(FuzzRenames.Shape.values()));
        appendSection(sb, "table sizes", sizes, null);
        appendSection(sb, "column roles", roles, null);
        appendSection(sb, "type x sorted x order x mode", typeRoleSort, null);
        return sb.toString();
    }

    private static Map<String, Integer> expectedTypes() {
        final Map<String, Integer> expected = new LinkedHashMap<>();
        for (final FuzzType type : FuzzType.ALL) {
            expected.put(type.label(), 0);
        }
        return expected;
    }

    private static <E extends Enum<E>> Map<String, Integer> expectedNames(final E[] values) {
        final Map<String, Integer> expected = new LinkedHashMap<>();
        for (final E value : values) {
            expected.put(value.name(), 0);
        }
        return expected;
    }

    private static void appendSection(
            final StringBuilder sb,
            final String title,
            final Map<String, Integer> counts,
            final Map<String, Integer> expected) {
        sb.append("//   ").append(title).append(": ");
        if (counts.isEmpty()) {
            sb.append("(none)\n");
        } else {
            sb.append(counts).append('\n');
        }
        if (expected != null) {
            final StringBuilder missing = new StringBuilder();
            for (final String key : expected.keySet()) {
                if (!counts.containsKey(key)) {
                    missing.append(key).append(' ');
                }
            }
            if (missing.length() > 0) {
                sb.append("//     NOT COVERED: ").append(missing).append('\n');
            }
        }
    }
}
