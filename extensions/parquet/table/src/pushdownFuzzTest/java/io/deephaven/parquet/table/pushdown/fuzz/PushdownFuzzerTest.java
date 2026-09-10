//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown.fuzz;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * DH-23557: a seeded fuzzer for predicate pushdown.
 *
 * <p>
 * Pushdown is a correctness-critical optimization -- a handler that wrongly reports "no overlap" silently drops rows,
 * with no user-visible signal. Existing coverage is a few dozen hand-written {@code (type, filter, layout)} triples;
 * this bench samples that space instead, checking every sample against the one oracle we trust: the same filter applied
 * to {@code diskTable.select()}, where no pushdown path exists.
 *
 * <p>
 * <strong>Not run by CI.</strong> {@code check} builds this source set (so it cannot rot) but never runs it, and the
 * root {@code nightly} task matches test tasks by name, which this one is not. Run it explicitly:
 *
 * <pre>
 * ./gradlew :extensions-parquet-table:pushdownFuzzTest -PforceTest=true
 * ./gradlew :extensions-parquet-table:pushdownFuzzTest -PforceTest=true \
 *     -DPushdownFuzzer.baseSeed=-1 -DPushdownFuzzer.cases=10000
 * </pre>
 *
 * <p>
 * Every case echoes {@code // Seed: <n>L} <em>before</em> it runs, so a crash or a hang still names its seed. Replay
 * one (or several) with {@code -DPushdownFuzzer.seeds=<seed>[,<seed>...]}.
 *
 * <p>
 * Note that a case seed is <strong>not</strong> usable as a {@code baseSeed}: case seeds are drawn from
 * {@code new Random(baseSeed).nextLong()}, and {@code new Random(s).nextLong() != s}, so
 * {@code -DPushdownFuzzer.baseSeed=<caseSeed>} would run a different case. Use {@code seeds} to replay, and
 * {@code startCase} to resume a truncated stream.
 */
public class PushdownFuzzerTest {

    /**
     * Seeds that have found real bugs and are <em>expected to pass</em> once fixed. Add a seed here after its
     * underlying defect is fixed, so it becomes a permanent regression case that {@link #testInterestingSeeds()}
     * replays.
     *
     * <p>
     * Every finding this bench produces is expected to be reduced to a standalone regression test and fixed, so a seed
     * lands here once its fix is in; see {@code findings/} in this package for the per-finding write-ups.
     */
    private static final long[] INTERESTING_SEEDS = {
            // Finding 3: a pre-epoch LocalDateTime with a sub-second part could be written to parquet but not read
            // back, because the materializers split the epoch offset with truncating / and %. Reaching it also
            // required findings 1 and 2 to be fixed, so this seed exercises all three.
            8750790217018904276L,
            // Finding 6: or(isNotNull(renamed), isNull(unrenamed)) over a DeferredViewTable tripped
            // MatchFilter.renameFilter's totality assertion on the partial rename map.
            -4715342832495625892L,
            // Finding 7: a formula filter pushed through nested renaming deferred views lost every renaming but
            // the last, because ConditionFilter.renameFilter replaced its name map instead of composing it.
            7177646707619336702L, 273087235408284003L, 8429452456633422855L,
            8540064508133314173L, 8922140309403778699L, -4605199251911937283L,
            8576325184258344286L, 7496982466862244149L,
            // Finding 9: parquet-space names leaked into two table-space APIs in ParquetTableLocation -- the
            // recorded sorting column, and the dictionary path's column-location lookup.
            -5347797226962475569L, -6688467811848818630L, -7423979211207825555L,
            // Finding 10: a deferred renameColumns that reassigns a name another of its columns reads from was
            // applied with view(), which is sequential, so a swap/chain/rotation produced the wrong data.
            5201278404043255708L, 2423783905725303439L, 5559549332320180280L,
            -8144732105314013200L, 5702961989472887051L,
            // Finding 11: SourceTable published every column of a multi-column sort as independently sorted, so a
            // composite data index's trailing column carried a false sortedness claim and over-returned rows.
            1681357320861610709L,
            // Finding 17: sorted-column match pushdown answered `!= NaN` with an ordering binary search, whose
            // notion of float equality differs from the filter's, dropping the NaN rows.
            -5472033891179623763L,
            // Finding 18: a location's data index read back as BigInteger for a scale-0 BigDecimal column, so
            // matching against it silently found nothing and the negation returned everything.
            -7982720036514329702L,
            // Finding 16 (bench): a table-wide sort claim on a partitioned layout, which partitioning invalidates.
            -1220343102263136052L, -3258625118121555365L,
            // Finding 12: a partition value containing a colon made the partition directory's relative URI
            // unparseable, so LocalTime and Instant partitioning columns could not be written at all.
            -2281078010550439077L, -1767017146706312469L, 3579704455286775782L,
            // Finding 13: filterColumnToManagerColumnName is not injective when a filter names a column and an
            // alias of it, so inverting it into renameColumns pairs gave two pairs with the same source.
            428667830982598836L, 6656699729815370963L,
            // Finding 19: a case-insensitive match filter whose value list contained a null threw for one, two
            // or three values and silently dropped the null for four or more.
            -3193954278432445066L, -3417280133301762829L, -4791489248932458532L, -8415955675519703733L,
    };

    private static final String ROOT_FILENAME = PushdownFuzzerTest.class.getName() + "_root";

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private File rootFile;
    private boolean savedMemoizeResults;

    @Before
    public void setUp() {
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();

        // QueryTable.whereInternal memoizes a `where` whose filters can all memoize, keyed on the
        // filter list. Without this, applying the same filter twice on one table would return the
        // first result verbatim -- so a toggle change or an order permutation within a case would
        // silently compare a result against itself.
        savedMemoizeResults = QueryTable.setMemoizeResults(false);
    }

    @After
    public void tearDown() {
        QueryTable.setMemoizeResults(savedMemoizeResults);
        if (rootFile != null) {
            FileUtils.deleteRecursively(rootFile);
        }
    }

    @Test
    public void testFuzzer() {
        final FuzzConfig config = FuzzConfig.fromConfiguration();
        System.out.println("// PushdownFuzzer " + config);
        if (config.seeds.length > 0) {
            runExactSeeds(config, config.seeds, "requested seeds");
        } else {
            runStream(config);
        }
    }

    @Test
    public void testInterestingSeeds() {
        if (INTERESTING_SEEDS.length == 0) {
            return;
        }
        runExactSeeds(FuzzConfig.fromConfiguration(), INTERESTING_SEEDS, "known-interesting seeds");
    }

    /** Run exactly these case seeds. This is the replay path; see the note on seeds vs. baseSeed above. */
    private void runExactSeeds(final FuzzConfig config, final long[] seeds, final String what) {
        final List<String> failures = new ArrayList<>();
        for (final long seed : seeds) {
            System.out.println("// Seed: " + seed + "L");
            final PushdownFuzzHarness.Result result = PushdownFuzzHarness.runCase(seed, rootFile, config);
            if (!result.ok()) {
                failures.add(result.message());
            }
        }
        if (!failures.isEmpty()) {
            fail(failures.size() + " of " + seeds.length + " " + what + " failed:\n"
                    + String.join("\n\n", failures));
        }
    }

    /**
     * The run loop.
     *
     * <p>
     * Termination is by case count, by wall-clock budget, or both -- whichever is reached first. The budget is checked
     * only <em>between</em> cases, so the in-flight case always completes and a truncated run never leaves a
     * half-reported case. The summary prints the next case index, so {@code -DPushdownFuzzer.startCase=<n>} with the
     * same base seed continues the identical stream.
     */
    private void runStream(final FuzzConfig config) {
        final Random seedStream = new Random(config.baseSeed);
        // Advance the stream to startCase so resumption reproduces the same seeds.
        for (int ii = 0; ii < config.startCase; ++ii) {
            seedStream.nextLong();
        }

        final long deadlineNanos = config.maxMinutes > 0
                ? System.nanoTime() + TimeUnit.MINUTES.toNanos(config.maxMinutes)
                : Long.MAX_VALUE;

        final List<String> failures = new ArrayList<>();
        final List<Long> failingSeeds = new ArrayList<>();
        final FuzzCoverage coverage = new FuzzCoverage();
        final long startNanos = System.nanoTime();

        int index = config.startCase;
        int casesRun = 0;
        while ((config.cases < 0 || index < config.startCase + config.cases)
                && System.nanoTime() < deadlineNanos) {
            final long caseSeed = seedStream.nextLong();
            // Echoed before the case runs: a crash or hang still names its seed.
            System.out.println("// Seed: " + caseSeed + "L");

            coverage.record(FuzzCase.generate(caseSeed, config));
            final PushdownFuzzHarness.Result result =
                    PushdownFuzzHarness.runCase(caseSeed, rootFile, config);
            ++casesRun;
            ++index;

            if (!result.ok()) {
                failingSeeds.add(caseSeed);
                final String message = "case seed " + caseSeed + "L failed:\n" + result.message();
                if (config.failFast) {
                    System.out.println(coverage.report(casesRun));
                    fail(message);
                }
                failures.add(message);
                if (failures.size() >= config.maxFailures) {
                    break;
                }
            }
        }

        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        final StringBuilder summary = new StringBuilder();
        summary.append("\n// ---- PushdownFuzzer summary ----\n");
        summary.append("// cases run:   ").append(casesRun).append('\n');
        summary.append("// elapsed:     ").append(elapsedMillis).append(" ms\n");
        summary.append("// next index:  ").append(index)
                .append("   (resume with -DPushdownFuzzer.startCase=").append(index)
                .append(" -DPushdownFuzzer.baseSeed=").append(config.baseSeed).append(")\n");
        if (failingSeeds.isEmpty()) {
            summary.append("// failures:    none\n");
        } else {
            summary.append("// failures:    ").append(failingSeeds.size()).append('\n');
            summary.append("// failing seeds (replay with -DPushdownFuzzer.seeds=..., ")
                    .append("or paste into INTERESTING_SEEDS):\n//     ");
            for (final long seed : failingSeeds) {
                summary.append(seed).append("L, ");
            }
            summary.append('\n');
        }
        summary.append(coverage.report(casesRun));
        System.out.println(summary);

        if (!failures.isEmpty()) {
            fail(failures.size() + " failing case(s):\n\n" + String.join("\n\n", failures));
        }
    }
}
