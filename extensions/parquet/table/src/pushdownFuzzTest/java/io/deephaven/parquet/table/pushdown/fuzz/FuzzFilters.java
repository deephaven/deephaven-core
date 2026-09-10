//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Filter generation for the pushdown fuzzer.
 *
 * <p>
 * Every generated filter is paired with a human-readable description so a failure names the exact predicate. Filters
 * always reference a column by its {@link FuzzColumn#resultName()}, since that is the name space the filter is
 * evaluated in after the case's renames.
 *
 * <p>
 * Literals are drawn from a mix of the type's {@link FuzzType#interesting()} catalog and values actually present in the
 * column, so a range bound lands on a row-group statistic boundary often enough to matter, while still probing the
 * values that make handlers bail out (null sentinels, NaN, infinities).
 */
public final class FuzzFilters {

    /** One generated predicate. */
    public static final class FuzzFilter {
        private final Filter filter;
        private final String description;
        /** Which pushdown tier this predicate is expected to be served by, for cost-crossing groups. */
        private final Tier tier;

        FuzzFilter(final Filter filter, final String description, final Tier tier) {
            this.filter = filter;
            this.description = description;
            this.tier = tier;
        }

        public Filter filter() {
            return filter;
        }

        public String description() {
            return description;
        }

        public Tier tier() {
            return tier;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    /**
     * A rough classification of which pushdown mechanism a predicate can reach. Used only to build filter groups whose
     * estimated costs cross between rounds, which is what exercises the re-sort and "bubble-up" paths in
     * {@code AbstractFilterExecution.executeStatelessFilter}.
     */
    public enum Tier {
        /** Range or match filter: statistics, dictionary, sorted, data index all possible. */
        METADATA,
        /** Chunk-filterable but not statistics-filterable: dictionary and null/constant regions only. */
        CHUNK_ONLY,
        /** No pushdown at all: multi-column formulas, virtual row variables. */
        NONE
    }

    private FuzzFilters() {}

    /** Generate one predicate over one of {@code columns}. */
    public static FuzzFilter generate(
            final Random random,
            final List<FuzzColumn> columns,
            final FuzzScope scope) {
        final FuzzColumn column = columns.get(random.nextInt(columns.size()));
        return generateFor(random, column, columns, scope);
    }

    /** Generate one predicate over {@code column} specifically. */
    public static FuzzFilter generateFor(
            final Random random,
            final FuzzColumn column,
            final List<FuzzColumn> columns,
            final FuzzScope scope) {
        final int kinds = 12;
        for (int attempt = 0; attempt < kinds * 2; ++attempt) {
            final FuzzFilter generated = tryKind(random.nextInt(kinds), random, column, columns, scope);
            if (generated != null) {
                return generated;
            }
        }
        // Every type supports null matching, so this always succeeds.
        return matchNull(random, column);
    }

    private static FuzzFilter tryKind(
            final int kind,
            final Random random,
            final FuzzColumn column,
            final List<FuzzColumn> columns,
            final FuzzScope scope) {
        switch (kind) {
            case 0:
                return equality(random, column, scope);
            case 1:
                return matchNull(random, column);
            case 2:
                return isNullFunction(random, column);
            case 3:
                return inList(random, column, scope);
            case 4:
                return caseInsensitiveIn(random, column, scope);
            case 5:
                return oneSidedRange(random, column, scope);
            case 6:
                return twoSidedRange(random, column, scope);
            case 7:
                return pattern(random, column, scope);
            case 8:
                return singleInputFormula(random, column);
            case 9:
                return multiInputFormula(random, column, columns);
            case 10:
                return crossTypeLiteral(random, column);
            case 11:
                return inverted(random, column, columns, scope);
            default:
                return null;
        }
    }

    // -------------------------------------------------------------------------------------------
    // Leaf kinds
    // -------------------------------------------------------------------------------------------

    private static FuzzFilter equality(final Random random, final FuzzColumn column, final FuzzScope scope) {
        final Object value = drawLiteral(random, column);
        final String literal = column.type().literal(value, scope);
        final String op = random.nextBoolean() ? "==" : "!=";
        final String text = column.resultName() + " " + op + " " + literal;
        return new FuzzFilter(RawString.of(text), text, Tier.METADATA);
    }

    private static FuzzFilter matchNull(final Random random, final FuzzColumn column) {
        final String text = column.resultName() + (random.nextBoolean() ? " == null" : " != null");
        return new FuzzFilter(RawString.of(text), text, Tier.METADATA);
    }

    private static FuzzFilter isNullFunction(final Random random, final FuzzColumn column) {
        final ColumnName name = ColumnName.of(column.resultName());
        if (random.nextBoolean()) {
            return new FuzzFilter(Filter.isNull(name), "isNull(" + column.resultName() + ")", Tier.CHUNK_ONLY);
        }
        return new FuzzFilter(Filter.isNotNull(name), "isNotNull(" + column.resultName() + ")", Tier.CHUNK_ONLY);
    }

    private static FuzzFilter inList(final Random random, final FuzzColumn column, final FuzzScope scope) {
        // Sizes 0..4+ matter: the primitive chunk match filters use `==` for 1-3 values and canonical
        // bit equality for larger sets, which differ for NaN and -0.0.
        final int count = random.nextInt(6);
        final List<String> literals = new ArrayList<>(count);
        for (int ii = 0; ii < count; ++ii) {
            // Deliberately allow a null element in the list.
            final Object value = random.nextInt(8) == 0 ? null : drawLiteral(random, column);
            literals.add(column.type().literal(value, scope));
        }
        final String op = random.nextBoolean() ? " in " : " not in ";
        final String text = column.resultName() + op + String.join(", ", literals);
        return new FuzzFilter(RawString.of(text), text, Tier.METADATA);
    }

    private static FuzzFilter caseInsensitiveIn(
            final Random random, final FuzzColumn column, final FuzzScope scope) {
        if (column.type() != FuzzType.STRING) {
            return null;
        }
        final int count = 1 + random.nextInt(3);
        final List<String> literals = new ArrayList<>(count);
        for (int ii = 0; ii < count; ++ii) {
            literals.add(column.type().literal(drawLiteral(random, column), scope));
        }
        final String op = random.nextBoolean() ? " icase in " : " icase not in ";
        final String text = column.resultName() + op + String.join(", ", literals);
        return new FuzzFilter(RawString.of(text), text, Tier.METADATA);
    }

    private static FuzzFilter oneSidedRange(final Random random, final FuzzColumn column, final FuzzScope scope) {
        if (column.type() == FuzzType.BOOLEAN) {
            return null;
        }
        final Object value = drawLiteral(random, column);
        if (value == null) {
            return null;
        }
        final String[] ops = {"<", "<=", ">", ">="};
        final String op = ops[random.nextInt(ops.length)];
        final String text = column.resultName() + " " + op + " " + column.type().literal(value, scope);
        return new FuzzFilter(RawString.of(text), text, Tier.METADATA);
    }

    private static FuzzFilter twoSidedRange(final Random random, final FuzzColumn column, final FuzzScope scope) {
        if (column.type() == FuzzType.BOOLEAN) {
            return null;
        }
        final Object lo = drawLiteral(random, column);
        final Object hi = drawLiteral(random, column);
        if (lo == null || hi == null) {
            return null;
        }
        final String loText = column.type().literal(lo, scope);
        final String hiText = column.type().literal(hi, scope);
        final String loOp = random.nextBoolean() ? ">" : ">=";
        final String hiOp = random.nextBoolean() ? "<" : "<=";
        if (random.nextBoolean()) {
            // Two separate range filters, conjoined -- flattened by ExtractInnerConjunctiveFilters
            // into two independently cost-sorted filters.
            final String a = column.resultName() + " " + loOp + " " + loText;
            final String b = column.resultName() + " " + hiOp + " " + hiText;
            return new FuzzFilter(Filter.and(RawString.of(a), RawString.of(b)),
                    "and(" + a + ", " + b + ")", Tier.METADATA);
        }
        // The single-expression form, which parses to one RangeFilter.
        final String text = loText + " " + flip(loOp) + " " + column.resultName() + " " + hiOp + " " + hiText;
        return new FuzzFilter(RawString.of(text), text, Tier.METADATA);
    }

    private static FuzzFilter pattern(final Random random, final FuzzColumn column, final FuzzScope scope) {
        if (column.type() != FuzzType.STRING) {
            return null;
        }
        final Object value = drawLiteral(random, column);
        if (value == null) {
            return null;
        }
        final String s = (String) value;
        // Keep the needle simple: this probes the pattern-filter pushdown path, not regex escaping.
        if (s.isEmpty() || !s.chars().allMatch(c -> c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
                || c >= '0' && c <= '9')) {
            return null;
        }
        final String needle = s.substring(0, 1 + random.nextInt(s.length()));
        final String[] methods = {"startsWith", "endsWith", "contains", "matches"};
        final String method = methods[random.nextInt(methods.length)];
        final String arg = "matches".equals(method) ? "`" + needle + ".*`" : "`" + needle + "`";
        final String text = column.resultName() + "." + method + "(" + arg + ")";
        return new FuzzFilter(RawString.of(text), text, Tier.CHUNK_ONLY);
    }

    private static FuzzFilter singleInputFormula(final Random random, final FuzzColumn column) {
        final String name = column.resultName();
        final String text;
        if (column.type() == FuzzType.INT || column.type() == FuzzType.LONG
                || column.type() == FuzzType.SHORT || column.type() == FuzzType.BYTE) {
            // isNull() guards the sentinel so the modulo does not silently treat null as a value.
            text = "!isNull(" + name + ") && " + name + " % 7 == 0";
        } else if (column.type() == FuzzType.DOUBLE || column.type() == FuzzType.FLOAT) {
            final String[] variants = {
                    "isNaN(" + name + ")",
                    "!isNaN(" + name + ")",
                    "isInf(" + name + ")",
                    "!isNull(" + name + ") && " + name + " > 0",
            };
            text = variants[random.nextInt(variants.length)];
        } else if (column.type() == FuzzType.STRING) {
            text = name + " != null && " + name + ".length() > " + random.nextInt(3);
        } else if (column.type() == FuzzType.BOOLEAN) {
            text = name + " != null && " + name;
        } else {
            text = name + " != null";
        }
        return new FuzzFilter(RawString.of(text), text, Tier.CHUNK_ONLY);
    }

    private static FuzzFilter multiInputFormula(
            final Random random, final FuzzColumn column, final List<FuzzColumn> columns) {
        // Needs a second column of a type whose null-ness is expressible the same way.
        final List<FuzzColumn> others = new ArrayList<>();
        for (final FuzzColumn candidate : columns) {
            if (candidate != column && candidate.type() == column.type()) {
                others.add(candidate);
            }
        }
        if (others.isEmpty()) {
            return null;
        }
        final FuzzColumn other = others.get(random.nextInt(others.size()));
        final String text = column.resultName() + " != null && " + other.resultName() + " != null && "
                + column.resultName() + " == " + other.resultName();
        return new FuzzFilter(RawString.of(text), text, Tier.NONE);
    }

    private static FuzzFilter crossTypeLiteral(final Random random, final FuzzColumn column) {
        // Deliberately mismatched literal types. Both tables must agree -- including agreeing to throw.
        final String name = column.resultName();
        final String text;
        switch (column.type().label()) {
            case "BigDecimal":
            case "BigInteger":
                // Integer and double literals against a big-number column: a real, supported path.
                text = name + (random.nextBoolean() ? " < 500" : " >= 500.0");
                break;
            case "int":
            case "short":
            case "byte":
                // A long literal beyond the column's range.
                text = name + " < " + (random.nextBoolean() ? "3000000000L" : "-3000000000L");
                break;
            case "double":
                text = name + " > 1";
                break;
            case "float":
                text = name + " > 1.0";
                break;
            default:
                return null;
        }
        return new FuzzFilter(RawString.of(text), text, Tier.METADATA);
    }

    private static FuzzFilter inverted(
            final Random random,
            final FuzzColumn column,
            final List<FuzzColumn> columns,
            final FuzzScope scope) {
        // Filter.not is a delegating wrapper; BasePushdownFilterContextImpl unwraps it before
        // extracting a range/match filter, so whether inversion survives into the handlers matters.
        final FuzzFilter inner = tryKind(random.nextInt(8), random, column, columns, scope);
        if (inner == null) {
            return null;
        }
        return new FuzzFilter(Filter.not(inner.filter()), "not(" + inner.description() + ")", inner.tier());
    }

    // -------------------------------------------------------------------------------------------
    // Composites
    // -------------------------------------------------------------------------------------------

    /** Combine 2-3 leaves with and/or/not, mixing columns and types. */
    public static FuzzFilter composite(
            final Random random,
            final List<FuzzColumn> columns,
            final FuzzScope scope) {
        final int count = 2 + random.nextInt(2);
        final List<Filter> parts = new ArrayList<>(count);
        final List<String> descriptions = new ArrayList<>(count);
        Tier tier = Tier.METADATA;
        for (int ii = 0; ii < count; ++ii) {
            final FuzzFilter leaf = generate(random, columns, scope);
            parts.add(leaf.filter());
            descriptions.add(leaf.description());
            if (leaf.tier() == Tier.NONE) {
                tier = Tier.NONE;
            } else if (leaf.tier() == Tier.CHUNK_ONLY && tier == Tier.METADATA) {
                tier = Tier.CHUNK_ONLY;
            }
        }
        final int form = random.nextInt(3);
        if (form == 0) {
            return new FuzzFilter(Filter.and(parts), "and(" + String.join(", ", descriptions) + ")", tier);
        }
        if (form == 1) {
            // An `or` is NOT flattened, so it stays one multi-column filter -- the shared-PPM path.
            return new FuzzFilter(Filter.or(parts), "or(" + String.join(", ", descriptions) + ")", tier);
        }
        return new FuzzFilter(Filter.not(Filter.or(parts)),
                "not(or(" + String.join(", ", descriptions) + "))", tier);
    }

    // -------------------------------------------------------------------------------------------
    // Literal selection
    // -------------------------------------------------------------------------------------------

    /**
     * Draw a filter literal. Mixes the type's interesting catalog with values actually present in the column, so that
     * bounds land on real row-group statistic boundaries as well as on the values that make handlers bail out.
     */
    private static Object drawLiteral(final Random random, final FuzzColumn column) {
        final List<Object> interesting = column.type().interesting();
        final List<Object> present = column.values();
        final int roll = random.nextInt(10);
        if (roll < 5 || present.isEmpty()) {
            return interesting.get(random.nextInt(interesting.size()));
        }
        if (roll < 9) {
            return present.get(random.nextInt(present.size()));
        }
        // The column's own extremes: a bound exactly equal to a row-group min or max.
        final List<Object> nonNull = new ArrayList<>();
        for (final Object value : present) {
            if (value != null) {
                nonNull.add(value);
            }
        }
        if (nonNull.isEmpty()) {
            return null;
        }
        nonNull.sort(column.type().comparator());
        return random.nextBoolean() ? nonNull.get(0) : nonNull.get(nonNull.size() - 1);
    }

    private static String flip(final String op) {
        return ">".equals(op) ? "<" : "<=";
    }
}
