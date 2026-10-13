//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.test.types;

/**
 * A JUnit category for tests that must be run in serial.
 *
 * If your test launches N or more work-heavy threads, you should be using SerialTest, since you'll likely be pegging
 * your cpu.
 *
 * That is, running a thread hog during the parallel test task just causes resource starvation, and large system-wide
 * pauses while contending to get execution time for a completely unrelated thread on the system.
 *
 * The 'testSerial' task created for these tests additionally have .mustRunAfter semantics, whereby, in order of script
 * evaluation, each testSerial.mustRunAfter allOther"testSerial"Tests, as well as .mustRunAfter
 * allOtherTestTasksNotNamed"testSerial"; that is, all testSerial tasks run after all other Test tasks are complete, and
 * then they take turns running one after another (see TestTools.groovy, #addEngineTest)
 *
 * ALL BENCHMARKS OR HEAVY-HITTER TESTS SHOULD USE @Category(SerialTest.class);
 *
 * Until better automation is delivered, you may need to edit your ModName.gradle file and add
 * TestTools.addEngineTest('Serial', false) to have this task created for you.
 *
 * Include: `dependencies { testCompile TestTools.projectDependency(project, 'Util') }` to add SerialTest to your
 * classpath if it is not available.
 *
 * The engine fixtures (RefreshingTableTestCase / QueryTableTestBase) are JUnit 4: extend one and mark test methods with
 * {@code @Test}. If the class already has a supertype, declare
 * {@code @Rule public final EngineCleanup base = new EngineCleanup();} instead of extending.
 */
public interface SerialTest {
}
