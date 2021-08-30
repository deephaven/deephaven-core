package io.deephaven.test.types;

/**
 * A JUnit category for tests that can be run in parallel.
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
public interface ParallelTest {
}
