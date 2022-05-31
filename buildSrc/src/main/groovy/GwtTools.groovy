import de.esoco.gwt.gradle.GwtLibPlugin
import de.esoco.gwt.gradle.GwtPlugin
import de.esoco.gwt.gradle.extension.GwtExtension
import de.esoco.gwt.gradle.task.GwtCompileTask
import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.compile.JavaCompile

/**
 * Helper to simplify / centralize configuring gwt plugins in build files
 */
@CompileStatic
class GwtTools {

    static GwtExtension gwtLib(Project p) {
        p.plugins.apply(GwtLibPlugin)
        GwtExtension ext = p.extensions.getByType(GwtExtension)
        applyDefaults(p, ext)
        return ext
    }
    static GwtExtension gwtCompile(Project p, String module, String description) {
        p.plugins.apply(GwtPlugin)
        GwtExtension ext = p.extensions.getByType(GwtExtension)
        applyDefaults(p, ext, true)

        // Apply our module settings to and gwtc task;
        // currently, there should only be one such task,
        // but we used to have two, and may have two again later,
        // so we'll leave this setup to be friendly-for-reuse
        p.tasks.withType(GwtCompileTask).all {
            GwtCompileTask gwtc ->
                applyModuleSettings p, gwtc, module,description
        }

        return ext
    }

    static void applyModuleSettings(Project p, GwtCompileTask gwtc, String mod, String description) {
        gwtc.onlyIf WebTools.&shouldRun
        boolean gwtDev = p.findProperty('gwtDev') == 'true'
        String extras = new File(p.buildDir, "gwt/dhapi/extra").absolutePath

        GwtExtension gwt = p.extensions.findByType(GwtExtension)

        gwt.with {
            module "${mod}${gwtDev ? 'Dev' : ''}"
            compile.with {
                style = 'PRETTY'
                generateJsInteropExports = true
                // TODO move this down a line when we want to give clients js that is not super strict / rigged to blow
                checkAssertions = true
                if (gwtDev) {
                    saveSource = true
                    extra = extras
                    logLevel = 'INFO'
                    draftCompile = true
                }
            }
        }

        p.gradle.projectsEvaluated {
            addGeneratedSources(p, gwtc)
        }

        gwtDev && gwtc.doFirst {
            gwtc.logger.quiet('Running in gwt dev mode; saving source to {}/dh/src', extras)
        }

        p.tasks.findByName('gwtCheck')?.enabled = false
    }

    static void applyDefaults(Project p, GwtExtension gwt, boolean compile = false) {
        gwt.gwtVersion = Classpaths.GWT_VERSION
        gwt.jettyVersion = Classpaths.JETTY_VERSION
        if (compile) {

            String warPath = new File(p.buildDir, 'gwt').absolutePath

            gwt.compile.with {
                // See https://github.com/esoco/gwt-gradle-plugin for all options
                /** The level of logging detail (ERROR, WARN, INFO, TRACE, DEBUG, SPAM, ALL) */
                logLevel = "INFO"
                /** The workDir, where we'll save gwt unitcache.  Use /tmp to avoid cluttering jenkins caches*/
                workDir = new File(System.getProperty("java.io.tmpdir"), "gwtWork")
                /** Where to write output files */
                war = warPath
                /** Compile a report that tells the "Story of Your Compile". */
                compileReport = true
                /** Compile quickly with minimal optimizations. */
                draftCompile = false
                /** Include assert statements in compiled output. */
                checkAssertions = false
                /** Script output style. (OBF, PRETTY, DETAILED)*/
                style = "OBF"
                /** Sets the optimization level used by the compiler. 0=none 9=maximum. */
                optimize = 9
                /** Fail compilation if any input file contains an error. */
                strict = true
                /** Specifies Java source level. ("1.6", "1.7")*/
                sourceLevel = "1.8"
                /** The number of local workers to use when compiling permutations. */
                localWorkers = 1
                /** Emit extra information allow chrome dev tools to display Java identifiers in many places instead of JavaScript functions. (NONE, ONLY_METHOD_NAME, ABBREVIATED, FULL)*/
//        methodNameDisplayMode = "NONE"

                /** Java args */
                maxHeapSize = "1024m"
                minHeapSize = "512m"
            }

            gwt.dev.with {
                /** The ip address of the code server. */
                bindAddress = "127.0.0.1"
                /** The port where the code server will run. */
                port = 9876
                /** Specifies Java source level ("1.6", "1.7").
                 sourceLevel = "1.8"
                 /** The level of logging detail (ERROR, WARN, INFO, TRACE, DEBUG, SPAM, ALL) */
                logLevel = "INFO"
                /** Emit extra information allow chrome dev tools to display Java identifiers in many placesinstead of JavaScript functions. (NONE, ONLY_METHOD_NAME, ABBREVIATED, FULL) */
                methodNameDisplayMode = "NONE"
                /** Where to write output files */
                war = warPath
//                    extraArgs = ["-firstArgument", "-secondArgument"]
            }


        }
    }

    static void addGeneratedSources(Project project, GwtCompileTask gwtc) {
        if (project.configurations.getByName(JavaPlugin.ANNOTATION_PROCESSOR_CONFIGURATION_NAME).dependencies) {
            (gwtc.src as ConfigurableFileCollection).from(
                (project.tasks.getByName(JavaPlugin.COMPILE_JAVA_TASK_NAME) as JavaCompile).options.annotationProcessorGeneratedSourcesDirectory
            )
        }
        project.configurations.getByName(JavaPlugin.COMPILE_CLASSPATH_CONFIGURATION_NAME).allDependencies.withType(ProjectDependency)*.dependencyProject*.each {
            Project p -> addGeneratedSources(p, gwtc)
        }
    }

}
