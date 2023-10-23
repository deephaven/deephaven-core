package io.deephaven.tools


import org.gradle.api.Project
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.compile.JavaCompile

/**
 */
class Tools {
    /**
     *  A dirty trick for IDE support... IJ with annotation processing enabled can have troubles w/ duplicated generated files.
     *  So, we tell gradle to use the same output directory that IntelliJ would have used, so there is only ever one generated source file.
     *
     */
    static void applyAnnotationProcessorPathHacks(Project p, boolean testMode = false) {
        p.tasks.named(testMode ? 'compileTestJava' : 'compileJava', JavaCompile) {
            JavaCompile javac ->
                // We'll use a gradle property, -PuseIdeaPaths=<default true>
                // In CI, we'll pass false, so all our output goes into build/, and we don't have to special-case sync these files.
                def ideaPaths = p.findProperty('useIdeaPaths') ?: 'true'

                // make sure the property value is registered as a javac task input; we definitely want to rerun if this changes
                javac.inputs.property('useIdeaPaths', ideaPaths)

                if (ideaPaths == 'true') {
                    // pick the matches-intellij path for this main / test sourceset
                    String path = testMode ? 'out/test/classes/generated_tests' : 'out/production/classes/generated'

                    File outputDir = p.file(path)
                    // If system is set to use intellij paths, lets make gradle point to same annotation
                    // processor output location as intellij, so we don't wind up with duplicated classes
                    // when intellij picks up its own annotation processor output plus gradle's.
                    javac.options.generatedSourceOutputDirectory.set outputDir
                    // make sure this directory always exists.
                    p.mkdir(outputDir)
                    // Make sure we clean our output whenever build/ is deleted.
                    (p.tasks.clean as Delete).delete(path)
                }
        }
    }

}
