package io.deephaven.test.types;

/**
 * A JUnit category for tests that must be run in serial.
 *
 * If your test launches N or more work-heavy threads, you should be using SerialTest, since you'll
 * likely be pegging your cpu.
 *
 * That is, running a thread hog during the parallel test task just causes resource starvation, and
 * large system-wide pauses while contending to get execution time for a completely unrelated thread
 * on the system.
 *
 * The 'testSerial' task created for these tests additionally have .mustRunAfter semantics, whereby,
 * in order of script evaluation, each testSerial.mustRunAfter allOther"testSerial"Tests, as well as
 * .mustRunAfter allOtherTestTasksNotNamed"testSerial"; that is, all testSerial tasks run after all
 * other Test tasks are complete, and then they take turns running one after another (see
 * TestTools.groovy, #addDbTest)
 *
 * ALL BENCHMARKS OR HEAVY-HITTER TESTS SHOULD USE @Category(SerialTest.class);
 *
 * Until better automation is delivered, you may need to edit your ModName.gradle file and add
 * TestTools.addDbTest('Serial', false) to have this task created for you.
 *
 * Include: `dependencies { testCompile TestTools.projectDependency(project, 'Util') }` to add
 * SerialTest to your classpath if it is not available.
 *
 * To convert a JUnit 3 base test case (henceforth TestType) to JUnit 4, you must: a) annotate any
 * testMethods() in TestType with @Test (junit 3 selects test methods based on
 * nameStartsWith("test")) b) override the JUnit 3 test case into a new supertype (henceforth:
 * AdapterType) b-1) [Required] Add a public no-op method beginning with text `test` in your
 * AdapterType, so it is a "valid JUnit 3 test" b-2) [Recommended] Add methods with @Before
 * and @After to call setUp() / tearDown() on the AdapterType field c) create a field assigned to an
 * instance of AdapterType (you may want simple instance field, an @Rule instance field, or
 * an @ClassRule static field) c-1) If your AdapterType has setUp/tearDown, and you are not
 * using @Rule or @ClassRule, annotate methods in your TestType: Add @Before/@After in TestType
 * which calls this.adapter.setUp()/tearDown() as appropriate. d) add an appropriate junit runner.
 * If you do not need anything fancy, use:
 * {@code @RunWith(org.junit.runners.BlockJUnit4ClassRunner.class)}
 *
 * If you extend and use our existing JUnit3 test fixtures like LiveTableTestCase, you can use
 * {@link io.deephaven.test.junit4.JUnit4LiveTableTestCase} as an example.
 */
@SuppressWarnings("JavadocReference")
public interface SerialTest {
}
