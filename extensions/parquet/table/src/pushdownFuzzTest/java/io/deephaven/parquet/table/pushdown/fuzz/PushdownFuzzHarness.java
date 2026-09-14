//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.api.filter.Filter;
import io.deephaven.base.FileUtils;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.util.SafeCloseable;

import java.io.File;
import java.nio.file.Path;

/**
 * Runs one fuzz case: build, write, read, and compare every generated filter against the in-memory oracle.
 *
 * <p>
 * The oracle is {@code diskTable.select()} -- an in-memory copy, taken <em>after</em> all renames and post-read
 * transforms so both sides are in the same name space. It is a valid witness precisely because no pushdown path exists
 * on it: {@code select()} produces {@code InMemoryColumnSource}s whose {@code estimatePushdownFilterCost} returns
 * {@code UNSUPPORTED_ACTION_COST}, and the sorted-columns attribute is not among the attributes copied by
 * {@code Select}. The attribute is stripped explicitly anyway, so that a future change to the copy rules cannot
 * silently make the oracle share the code under test.
 */
public final class PushdownFuzzHarness {

    /** Why a case failed, or {@link #OK}. */
    public static final class Result {
        public static final Result OK = new Result(null, null);

        private final String message;
        private final Throwable cause;

        private Result(final String message, final Throwable cause) {
            this.message = message;
            this.cause = cause;
        }

        static Result failure(final String message, final Throwable cause) {
            return new Result(message, cause);
        }

        public boolean ok() {
            return message == null;
        }

        public String message() {
            return message;
        }

        public Throwable cause() {
            return cause;
        }
    }

    private PushdownFuzzHarness() {}

    /**
     * Run one case to completion.
     *
     * @param seed the case seed; also the only thing needed to reproduce it
     * @param root a directory the case may create and delete a subdirectory in
     * @param config the run knobs
     * @return {@link Result#OK}, or a failure carrying the full case description
     */
    public static Result runCase(final long seed, final File root, final FuzzConfig config) {
        final FuzzCase fuzzCase = FuzzCase.generate(seed, config);
        final File caseDir = new File(root, "case_" + Long.toUnsignedString(seed, 16));

        // Each case gets its own liveness scope so a long run does not accumulate table state.
        try (final SafeCloseable scope = LivenessScopeStack.open(new LivenessScope(true), true);
                final FuzzToggles toggles = fuzzCase.toggles().apply()) {
            if (!caseDir.mkdirs() && !caseDir.isDirectory()) {
                return Result.failure("could not create case directory " + caseDir, null);
            }
            return runCaseBody(fuzzCase, caseDir);
        } catch (final Throwable t) {
            return Result.failure(fuzzCase.describe() + "unexpected failure: " + chain(t), t);
        } finally {
            FileUtils.deleteRecursively(caseDir);
        }
    }

    private static Result runCaseBody(final FuzzCase fuzzCase, final File caseDir) {
        final String description = fuzzCase.describe();

        // --- write ------------------------------------------------------------------------------
        final Table source = fuzzCase.layout().sortForWrite(
                fuzzCase.buildSourceTable(), fuzzCase.sortColumn());
        final ParquetInstructions writeInstructions =
                fuzzCase.layout().writeInstructions(fuzzCase.columns(), fuzzCase.tableSize());

        final String readPath;
        try {
            readPath = fuzzCase.layout().write(
                    source, caseDir.getPath(), writeInstructions, fuzzCase.columns(), fuzzCase.sortColumn());
        } catch (final RuntimeException e) {
            // A write failure is a real finding, but it is a write/read finding rather than a
            // pushdown one; say so explicitly rather than blaming the filter.
            return Result.failure(description + "parquet write failed: " + chain(e), e);
        }

        // --- read -------------------------------------------------------------------------------
        Table diskTable;
        try {
            if (fuzzCase.renames().hasInstructionMappings()) {
                final ParquetInstructions readInstructions = fuzzCase.renames()
                        .applyInstructionMappings(new ParquetInstructions.Builder())
                        .build();
                diskTable = ParquetTools.readTable(readPath, readInstructions);
            } else {
                diskTable = ParquetTools.readTable(readPath);
            }
            diskTable = fuzzCase.renames().applyTableRenames(diskTable);
            diskTable = fuzzCase.layout().applyAfterRead(diskTable, fuzzCase.sortColumn());
            diskTable = fuzzCase.layout().applySelection(diskTable);
        } catch (final RuntimeException e) {
            return Result.failure(description + "parquet read failed: " + chain(e), e);
        }

        // --- oracle -----------------------------------------------------------------------------
        final Table memTable;
        try {
            memTable = diskTable.select().withoutAttributes(
                    java.util.Collections.singleton(Table.SORTED_COLUMNS_ATTRIBUTE));
        } catch (final RuntimeException e) {
            return Result.failure(description + "building the in-memory oracle failed: " + chain(e), e);
        }

        // Sanity gate: if the unfiltered tables already differ, this is a read/write bug and not a
        // pushdown bug. Reporting it as such saves a lot of misdirected debugging.
        try {
            TstUtils.assertTableEquals(
                    description + "UNFILTERED disk vs memory differ -- this is a parquet read/write bug, "
                            + "not a pushdown bug",
                    diskTable, memTable);
        } catch (final AssertionError | RuntimeException e) {
            return Result.failure(description + "unfiltered mismatch: " + e, e);
        }

        // --- filters ----------------------------------------------------------------------------
        for (final FuzzFilters.FuzzFilter fuzzFilter : fuzzCase.filters()) {
            final Result result = compareFilter(description, diskTable, memTable, fuzzFilter);
            if (!result.ok()) {
                return result;
            }
        }
        return Result.OK;
    }

    /**
     * Apply one filter to both tables and require agreement.
     *
     * <p>
     * Empty results are legitimate -- a fuzzer generates plenty of predicates nothing matches -- so this is "allow
     * empty" throughout. If the in-memory side throws, the disk side must throw the same exception type: a filter that
     * is invalid for a column's type has to fail identically whether or not pushdown is involved.
     */
    private static Result compareFilter(
            final String description,
            final Table diskTable,
            final Table memTable,
            final FuzzFilters.FuzzFilter fuzzFilter) {
        final Filter filter = fuzzFilter.filter();
        final String context = description + "filter=" + fuzzFilter.description() + "\n";

        Table memResult = null;
        Throwable memThrown = null;
        try {
            memResult = memTable.where(filter).coalesce();
        } catch (final RuntimeException | AssertionError t) {
            memThrown = t;
        }

        Table diskResult = null;
        Throwable diskThrown = null;
        try {
            diskResult = diskTable.where(filter).coalesce();
        } catch (final RuntimeException | AssertionError t) {
            diskThrown = t;
        }

        if (memThrown != null || diskThrown != null) {
            if (memThrown == null) {
                return Result.failure(context + "disk table threw but memory table did not: "
                        + chain(diskThrown), diskThrown);
            }
            if (diskThrown == null) {
                return Result.failure(context + "memory table threw but disk table did not: "
                        + chain(memThrown), memThrown);
            }
            if (!rootType(memThrown).equals(rootType(diskThrown))) {
                return Result.failure(context
                        + "both threw, but with different types: memory=" + rootType(memThrown)
                        + " disk=" + rootType(diskThrown)
                        + "\n  memory: " + chain(memThrown)
                        + "\n  disk:   " + chain(diskThrown), diskThrown);
            }
            // Both failed the same way; that is agreement.
            return Result.OK;
        }

        try {
            TstUtils.assertTableEquals(context, memResult, diskResult);
        } catch (final AssertionError | RuntimeException e) {
            return Result.failure(context + "filtered results differ: " + e
                    + diagnostics(diskTable, memTable, diskResult, memResult), e);
        }
        return Result.OK;
    }

    /**
     * The most specific meaningful exception type. Engine failures are frequently wrapped in a
     * {@code TableInitializationException}, and the wrapper is the same on both paths, so compare the cause when there
     * is one.
     */
    /**
     * Extra detail for a result mismatch: the definition both sides share, the sizes, and -- when
     * {@code PushdownFuzzer.dumpOnMismatch} is set -- the tables themselves. A mismatch is where the bench earns its
     * keep, so it is worth printing enough to start debugging from.
     */
    private static String diagnostics(
            final Table diskTable,
            final Table memTable,
            final Table diskResult,
            final Table memResult) {
        final StringBuilder sb = new StringBuilder();
        sb.append("\n  definition:   ").append(diskTable.getDefinition().getColumnNamesAsString());
        sb.append("\n  input sizes:  disk=").append(diskTable.size()).append(" mem=").append(memTable.size());
        sb.append("\n  result sizes: disk=").append(diskResult.size()).append(" mem=").append(memResult.size());
        sb.append("\n  disk attributes: ").append(diskTable.getAttributes());
        if (io.deephaven.configuration.Configuration.getInstance()
                .getBooleanWithDefault("PushdownFuzzer.dumpOnMismatch", false)) {
            sb.append("\n--- disk input ---\n").append(showToString(diskTable));
            sb.append("\n--- mem input ---\n").append(showToString(memTable));
            sb.append("\n--- disk result ---\n").append(showToString(diskResult));
            sb.append("\n--- mem result ---\n").append(showToString(memResult));
        }
        return sb.toString();
    }

    private static String showToString(final Table table) {
        return io.deephaven.engine.util.TableTools.string(table, 50);
    }

    /** Render a throwable and its whole cause chain; the outermost message is usually uninformative. */
    private static String chain(final Throwable thrown) {
        final StringBuilder sb = new StringBuilder();
        Throwable current = thrown;
        int depth = 0;
        while (current != null && depth < 8) {
            if (depth > 0) {
                sb.append("\n    caused by: ");
            }
            sb.append(current.getClass().getName()).append(": ").append(current.getMessage());
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
            ++depth;
        }
        return sb.toString();
    }

    private static String rootType(final Throwable thrown) {
        Throwable current = thrown;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current.getClass().getName();
    }

    /** The path a case writes into, for diagnostics. */
    public static String caseDirectory(final File root, final long seed) {
        return Path.of(root.getPath(), "case_" + Long.toUnsignedString(seed, 16)).toString();
    }
}
