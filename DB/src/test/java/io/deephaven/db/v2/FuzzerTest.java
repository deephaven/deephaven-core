package io.deephaven.db.v2;

import io.deephaven.base.FileUtils;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.util.GroovyDeephavenSession;
import io.deephaven.db.util.GroovyDeephavenSession.RunScripts;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.utils.RuntimeMemory;
import io.deephaven.db.v2.utils.TimeProvider;
import io.deephaven.test.junit4.JUnit4LiveTableTestCase;
import io.deephaven.test.types.SerialTest;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Category(SerialTest.class)
public class FuzzerTest {
    private static final String TEST_ROOT = System.getProperty("devroot", ".");
    private static final String DB_ROOT =
        TEST_ROOT + "/tmp/" + FuzzerTest.class.getSimpleName() + "_DBRoot";
    private static final boolean REALTIME_FUZZER_ENABLED =
        Configuration.getInstance().getBooleanWithDefault("FuzzerTest.realTime", false);

    JUnit4LiveTableTestCase framework = new JUnit4LiveTableTestCase();

    private static class FuzzDescriptor {
        final long querySeed;
        final long tableSeed;
        final int steps;
        final int sleepPerStep;

        private FuzzDescriptor(long querySeed, long tableSeed) {
            this(querySeed, tableSeed, 10, 100);
        }

        private FuzzDescriptor(long querySeed, long tableSeed, int steps, int sleepPerStep) {
            this.querySeed = querySeed;
            this.tableSeed = tableSeed;
            this.steps = steps;
            this.sleepPerStep = sleepPerStep;
        }
    }

    private static final List<FuzzDescriptor> INTERESTING_SEEDS = new ArrayList<FuzzDescriptor>() {
        {
            // Add interesting seed combinations here and they will automatically be tested.
            // Query seed, Table seed
        }
    };

    private void cleanupPersistence() {
        System.gc();
        System.gc();
        int tries = 0;
        boolean success = false;
        do {
            try {
                FileUtils.deleteRecursively(new File(DB_ROOT));
                success = true;
            } catch (Exception e) {
                System.gc();
                tries++;
            }
        } while (!success && tries < 10);
    }

    private void setupPersistence() {
        cleanupPersistence();
        // noinspection ResultOfMethodCallIgnored
        new File(DB_ROOT + File.separatorChar + "Definitions").mkdirs();
    }

    @Before
    public void setUp() throws Exception {
        framework.setUp();
        setupPersistence();
    }

    @After
    public void tearDown() throws Exception {
        QueryScope.setScope(new QueryScope.StandaloneImpl());
        cleanupPersistence();
        framework.tearDown();
    }

    private GroovyDeephavenSession getGroovySession() throws IOException {
        return getGroovySession(null);
    }

    private GroovyDeephavenSession getGroovySession(@Nullable TimeProvider timeProvider)
        throws IOException {
        final GroovyDeephavenSession session =
            new GroovyDeephavenSession(RunScripts.serviceLoader());
        QueryScope.setScope(session.newQueryScope());
        return session;
    }

    @Test
    public void testFuzzer() throws IOException, InterruptedException {
        testFuzzerScriptFile(0, "/DB/src/test/java/io/deephaven/db/v2/fuzzertest.groovy", true);
    }

    private void testFuzzerScriptFile(final long timeSeed, String s, boolean realtime)
        throws IOException, InterruptedException {
        final Random timeRandom = new Random(timeSeed);
        final String groovyString =
            FileUtils.readTextFile(new File(Configuration.getInstance().getDevRootPath() + s));

        final DBDateTime fakeStart = DBTimeUtils.convertDateTime("2020-03-17T13:53:25.123456 NY");
        final MutableLong now = new MutableLong(fakeStart.getNanos());
        final TimeProvider timeProvider = realtime ? null : () -> new DBDateTime(now.longValue());

        final GroovyDeephavenSession session = getGroovySession(timeProvider);

        System.out.println(groovyString);

        session.evaluateScript(groovyString);

        final List<Object> hardReferences = new ArrayList<>();

        validateBindingTableMapConstituents(session, hardReferences);
        validateBindingTables(session, hardReferences);
        annotateBinding(session);

        // so the first tick has a duration related to our initialization time
        if (!realtime) {
            now.add(DBTimeUtils.SECOND / 10 * timeRandom.nextInt(20));
        }

        final TimeTable timeTable = (TimeTable) session.getVariable("tt");

        for (int step = 0; step < 100; ++step) {
            final int fstep = step;
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                System.out.println("Step = " + fstep);
                timeTable.refresh();
            });
            if (realtime) {
                Thread.sleep(1000);
            } else {
                now.add(DBTimeUtils.SECOND / 10 * timeRandom.nextInt(20));
            }
        }
    }

    @Test
    public void testInterestingFuzzerSeeds() throws IOException, InterruptedException {
        final QueryFactory qf = new QueryFactory();
        for (final FuzzDescriptor fuzzDescriptor : INTERESTING_SEEDS) {
            System.gc();
            final GroovyDeephavenSession session = getGroovySession();
            final StringBuilder query = new StringBuilder();
            query.append(qf.getTablePreamble(fuzzDescriptor.tableSeed));
            query.append(qf.generateQuery(fuzzDescriptor.tableSeed));

            System.out.println("Running test=======================\n TableSeed: "
                + fuzzDescriptor.tableSeed + " QuerySeed: " + fuzzDescriptor.tableSeed);
            System.out.println(query.toString());

            session.evaluateScript(query.toString());

            annotateBinding(session);
            final List<Object> hardReferences = new ArrayList<>();
            validateBindingTables(session, hardReferences);

            final TimeTable timeTable = (TimeTable) session.getVariable("tt");
            for (int step = 0; step < fuzzDescriptor.steps; ++step) {
                final int fstep = step;
                LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                    System.out.println("Step = " + fstep);
                    timeTable.refresh();
                });
                Thread.sleep(fuzzDescriptor.sleepPerStep);
            }
        }
    }

    // @Test
    // public void testLargeFuzzerSeed() throws IOException, InterruptedException {
    // final int segmentSize = 50;
    // for (int firstRun = 0; firstRun < 100; firstRun += segmentSize) {
    // LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    // final int lastRun = firstRun + segmentSize - 1;
    // System.out.println("Performing runs " + firstRun + " to " + lastRun);
    //// runLargeFuzzerSetWithSeed(1583849877513833000L, firstRun, lastRun);
    // runLargeFuzzerSetWithSeed(1583865378974605000L, firstRun, lastRun);
    // System.gc();
    // }
    // }

    @Test
    public void testLargeSetOfFuzzerQueriesRealtime() throws IOException, InterruptedException {
        Assume.assumeTrue("Realtime Fuzzer can have a positive feedback loop.",
            REALTIME_FUZZER_ENABLED);
        runLargeFuzzerSetWithSeed(DBDateTime.now().getNanos(), 0, 99, true, 120, 1000);
    }

    @Test
    public void testLargeSetOfFuzzerQueriesSimTime() throws IOException, InterruptedException {
        final long seed1 = DBDateTime.now().getNanos();
        for (long iteration = 0; iteration < 5; ++iteration) {
            for (int segment = 0; segment < 10; segment++) {
                LiveTableMonitor.DEFAULT.resetForUnitTests(false);
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    System.out.println("// Segment: " + segment);
                    final int firstRun = segment * 10;
                    runLargeFuzzerSetWithSeed(seed1 + iteration, firstRun, firstRun + 10, false,
                        180, 0);
                }
            }
        }
    }

    private void runLargeFuzzerSetWithSeed(long mainTestSeed, int firstRun, int lastRun,
        boolean realtime, int stepsToRun, int sleepTime) throws IOException, InterruptedException {

        final QueryFactory qf = new QueryFactory();
        System.out.println("// TestSeed: " + mainTestSeed + "L");
        System.out.println("// FirstRun: " + firstRun);
        System.out.println("// LastRun: " + lastRun);
        final String tableQuery = qf.getTablePreamble(mainTestSeed);

        final Random sourceRandom = new Random(mainTestSeed);
        final Random timeRandom = new Random(mainTestSeed + 1);

        final DBDateTime fakeStart = DBTimeUtils.convertDateTime("2020-03-17T13:53:25.123456 NY");
        final MutableLong now = new MutableLong(fakeStart.getNanos());
        final TimeProvider timeProvider = () -> new DBDateTime(now.longValue());
        final long start = System.currentTimeMillis();

        final GroovyDeephavenSession session = getGroovySession(realtime ? null : timeProvider);

        System.out.println(tableQuery);

        session.evaluateScript(tableQuery);

        for (int runNum = 0; runNum <= lastRun; ++runNum) {
            final long currentSeed = sourceRandom.nextLong();

            final String query = qf.generateQuery(currentSeed);

            if (runNum >= firstRun) {
                final StringBuilder sb =
                    new StringBuilder("//========================================\n");
                sb.append("// Seed: ").append(currentSeed).append("L\n\n");
                sb.append(query).append("\n");
                System.out.println(sb.toString());
                session.evaluateScript(query);
            }

        }
        annotateBinding(session);

        if (!realtime) {
            now.add(DBTimeUtils.SECOND / 10 * timeRandom.nextInt(20));
        }

        final DecimalFormat commaFormat = new DecimalFormat();
        commaFormat.setGroupingUsed(true);
        final long startTime = System.currentTimeMillis();

        final long loopStart = System.currentTimeMillis();
        final TimeTable timeTable = (TimeTable) session.getVariable("tt");
        for (int step = 0; step < stepsToRun; ++step) {
            final int fstep = step;
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(timeTable::refresh);

            final long totalMemory = RuntimeMemory.getInstance().totalMemory();
            final long freeMemory = RuntimeMemory.getInstance().freeMemory();
            final long usedMemory = totalMemory - freeMemory;

            // noinspection unchecked,OptionalGetWithoutIsPresent
            final long maxTableSize = session.getBinding().getVariables().values().stream()
                .filter(x -> x instanceof Table).mapToLong(x -> ((Table) x).size()).max()
                .getAsLong();
            System.out.println(
                (System.currentTimeMillis() - startTime) + "ms: After Step = " + fstep + ", Used = "
                    + commaFormat.format(usedMemory) + ", Free = " + commaFormat.format(freeMemory)
                    + " / Total Memory: " + commaFormat.format(totalMemory) + ", TimeTable Size = "
                    + timeTable.size() + ", Largest Table: " + maxTableSize);

            if (realtime) {
                Thread.sleep(sleepTime);
            } else {
                now.add(DBTimeUtils.SECOND / 10 * timeRandom.nextInt(20));
            }
            if (maxTableSize > 500_000L) {
                System.out.println("Tables have grown too large, quitting fuzzer run.");
                break;
            }
        }

        final long loopEnd = System.currentTimeMillis();
        System.out.println(
            "Elapsed time: " + (loopEnd - start) + "ms, loop: " + (loopEnd - loopStart) + "ms"
                + (realtime ? ""
                    : (", sim: "
                        + (double) (now.longValue() - fakeStart.getNanos()) / DBTimeUtils.SECOND))
                + ", ttSize: " + timeTable.size());
    }

    private void annotateBinding(GroovyDeephavenSession session) {
        // noinspection unchecked
        session.getBinding().getVariables().forEach((k, v) -> {
            if (v instanceof Table) {
                ((Table) v).setAttribute("BINDING_VARIABLE_NAME", k);
            }
        });
    }

    private void addPrintListener(GroovyDeephavenSession session, final String variable,
        List<Object> hardReferences) {
        final Table table = (Table) session.getVariable(variable);
        System.out.println(variable);
        TableTools.showWithIndex(table);
        System.out.println();
        if (table.isLive()) {
            final FuzzerPrintListener listener = new FuzzerPrintListener(variable, table);
            ((DynamicTable) table).listenForUpdates(listener);
            hardReferences.add(listener);
        }
    }

    private void validateBindingTables(GroovyDeephavenSession session,
        List<Object> hardReferences) {
        // noinspection unchecked
        session.getBinding().getVariables().forEach((k, v) -> {
            if (v instanceof QueryTable && ((QueryTable) v).isRefreshing()) {
                addValidator(hardReferences, k.toString(), (QueryTable) v);
            }
        });
    }

    private void validateBindingTableMapConstituents(GroovyDeephavenSession session,
        List<Object> hardReferences) {
        // noinspection unchecked
        session.getBinding().getVariables().forEach((k, v) -> {
            if (v instanceof LocalTableMap && ((LocalTableMap) v).isRefreshing()) {
                for (final Object tablemapKey : ((LocalTableMap) v).getKeySet()) {
                    addValidator(hardReferences, k.toString() + "_" + tablemapKey,
                        (QueryTable) ((LocalTableMap) v).get(tablemapKey));
                }
                final TableMap.Listener listener = (key, table) -> {
                    addValidator(hardReferences, k.toString() + "_" + key, (QueryTable) table);
                };
                hardReferences.add(listener);
                ((LocalTableMap) v).addListener(listener);
            }
        });
    }

    private void addValidator(List<Object> hardReferences, String description, QueryTable v) {
        final TableUpdateValidator validator = TableUpdateValidator.make(description, v);
        final FailureListener listener = new FailureListener();
        validator.getResultTable().listenForUpdates(listener);
        hardReferences.add(listener);
    }
}
