import org.gradle.api.Project
import org.gradle.api.artifacts.Dependency
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.reporting.Report
import org.gradle.api.reporting.internal.SimpleReport
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.junit.JUnitOptions
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension
import org.gradle.testing.jacoco.tasks.JacocoReport
import org.gradle.testing.jacoco.tasks.JacocoReportsContainer

import java.util.concurrent.Callable

import static java.io.File.separator
import static org.gradle.api.plugins.JavaPlugin.TEST_RUNTIME_CLASSPATH_CONFIGURATION_NAME

/**
 * Various utilities for setting up test tasks in gradle.
 */
@SuppressWarnings("GroovyUnusedDeclaration") // used in gradle scripts.
//@CompileStatic
class TestTools {

    static final String TEST_GROUP = "~Deephaven Test"

    static Test addEngineSerialTest(Project project) {
        return addEngineTest(project, 'Serial', false, true)
    }

    static Test addEngineParallelTest(Project project) {
        // Add @Category(ParallelTest.class) to have your tests run in parallel
        // Note: Supports JUnit4 or greater only (you use @Test annotations to mark test methods).
        return addEngineTest(project, 'Parallel', true, false)
    }

    static Test addEngineOutOfBandTest(Project project) {
        return addEngineTest(project, 'OutOfBand', true, false)
    }

    private static Test addEngineTest(
        Project project,
        String type,
        boolean parallel = false,
        boolean isolated = false
    ) {
        Test mainTest = project.tasks.getByName('test') as Test
        Test t = project.tasks.create("test$type", Test)

        mainTest.useJUnit {
            JUnitOptions opts ->
                opts.excludeCategories 'io.deephaven.test.types.ParallelTest',
                        'io.deephaven.test.types.SerialTest',
                        'io.deephaven.test.types.OutOfBandTest'
                // TODO: keep this list uptodate when adding new test types.
                // This is a list of types excluded from basic test task.
        }
        t.useJUnit {
            JUnitOptions opts ->
                opts.includeCategories "io.deephaven.test.types.${type}Test"
        }
        t.with {
            // common configuration for this test task
            group = TEST_GROUP
            description = """Runs @Category(${t.name.capitalize()}.class)

By default only runs in CI; to run locally:
`CI=true ./gradlew test` or `./gradlew $t.name`"""
            dependsOn project.tasks.findByName('testClasses')

            if (parallel) {
                // We essentially want to set maxParallelForks for all "normal" tests. It will be capped by the number
                // of gradle workers, org.gradle.workers.max. We aren't able to set set this property for all Test types,
                // because testSerial needs special handling.
                maxParallelForks = Runtime.runtime.availableProcessors()
                if (project.hasProperty('forkEvery')) {
                    forkEvery = project.property('forkEvery') as int
                }
            } else {
                maxParallelForks = 1
                // == safe for strings in groovy
                if ('Serial' == type) { // testSerial special-casing:

                    // all testSerial tasks must take turns running, one after the other
                    project.rootProject.allprojects*.tasks*.findByName(t.name).findResults {
                        // Whenever each testSerial is declared, it mustRunAfter all other testSerial which currently exist.
                        it != t ? it : null
                    }.forEach t.&mustRunAfter

                    // all testSerial tasks must run after *all* test tasks not named testSerial
                    // wait until all build scripts are evaluated
                    project.gradle.projectsEvaluated {
                        // all testSerial tasks .mustRunAfter all other Test tasks whose name is not 'testSerial'.
                        project.rootProject.allprojects*.tasks*.withType(Test)*.findResults {
                            it?.name == t.name ? null : it // findResults compiles a new list of all non-null results
                        }.forEach t.&mustRunAfter // our testSerial task must run after all matched tests.
                    }

                } // end testSerial special-casing.
            }
            if (isolated) {
                t.forkEvery = 1
            }

            // wire up dependencies manually, since we don't get this for free in custom tasks
            // (it's usually assumed you will do a custom sourceSet for integration tests,
            // but we already use custom layouts which make "use separate sourcesets per module" in IntelliJ...troublesome).
            SourceSetContainer sources = project.getExtensions().findByType(JavaPluginExtension).sourceSets
            setClasspath project.files(sources.getByName('test').output, sources.getByName('main').output, project.configurations.getByName(TEST_RUNTIME_CLASSPATH_CONFIGURATION_NAME))

            // we also need to adjust the reporting output directory of the alt task,
            // so we don't stomp over top of previous reports.
            reports.all {
                Report report ->
                    String rebased = report.outputLocation.get().asFile.absolutePath
                            .replace "${separator}test$separator", "$separator$type$separator"
                    (report as SimpleReport).outputLocation.set new File(rebased)
            }
            // this is not part of the standard class; it is glued on later by jacoco plugin;
            // we want to give each test it's own output files for jacoco analysis,
            // so we don't accidentally stomp on previous output.
            // TODO: verify jenkins is analyzing _all_ information here.
            if (project.findProperty('jacoco.enabled') == "true") {
                (t['jacoco'] as JacocoTaskExtension).with {
                    destinationFile = project.provider({ new File(project.buildDir, "jacoco/${type}.exec".toString()) } as Callable<File>)
                    classDumpDir = new File(project.buildDir, "jacoco/${type}Dumps".toString())
                }
                (project['jacocoTestReport'] as JacocoReport).with {
                    reports {
                        JacocoReportsContainer c ->
                        c.xml.enabled = true
                        c.csv.enabled = true
                        c.html.enabled = true
                    }
                }
            }

        }
        if (project.findProperty('jacoco.enabled') == "true") {
            project.tasks.findByName('jacocoTestReport').mustRunAfter(t)
        }

        return t
    }

    static final boolean getIsCI() {
        // for developers to specify `CI=true`
        System.getenv('CI') == 'true' ||
        // make it work on jenkins, which does not currently have a CI env variable.
        System.getenv('JENKINS_HOME') != null
    }

    static final boolean getIsOfficial() {
        System.getenv('OFFICIAL_RELEASE') == 'true'
    }

    static Dependency projectDependency(Project project, String path) {
        return project.dependencies.project([
                path: ":${path - ':'}",
                configuration: 'testOutput'
        ])
    }

    static boolean shouldRunTests(Project p) {
        // allow `-x test` to disable *all* test tasks, even if they aren't named test.
        return !('test' in (p.gradle.startParameter.excludedTaskNames))
    }
}
