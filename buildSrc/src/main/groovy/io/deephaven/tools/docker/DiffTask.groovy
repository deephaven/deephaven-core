package io.deephaven.tools.docker

import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.Directory
import org.gradle.api.file.FileVisitDetails
import org.gradle.api.internal.file.FileLookup
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.util.PatternFilterable
import org.gradle.api.tasks.util.PatternSet

import javax.inject.Inject
import java.nio.file.Path

/**
 * Compares the files in two sets of contents, failing if there are differences. Intended as a
 * counterpart to the Synx task, to allow failing the build if certain sources don't match their
 * expected values.
 */
@CompileStatic
abstract class DiffTask extends DefaultTask {
    static class ActualContents {
        private Directory actualContents;
        private final PatternSet ignoreInActual = new PatternSet();

        /**
         * Specifies the contents to check to see if they match the expected files.
         */
        void directory(Directory actualContents) {
            this.actualContents = actualContents;
        }

        /**
         * Marks some contents as not needing to be diff'd, like Sync's "preserve". Counter-intuitively,
         * this usually requires an "exclude" to specify only a specific subdirectory should be diff'd.
         */
        void ignore(Action<? super PatternFilterable> action) {
            action.execute(this.ignoreInActual)
        }
    }
    // This is an Object because Gradle doesn't define an interface for getAsFileTree(), so we
    // will resolve it when we execute the task. This allows us to read from various sources,
    // such as configurations.
    @InputFiles
    abstract Property<Object> getExpectedContents()

    // In order to not treat every other task's input and output as this task's input, we process
    // the source directory and the ignore pattern as soon as they are set, to claim the actual
    // input files.
    private final Set<File> existingFiles = [];

    private final ActualContents actualContentsHolder = new ActualContents();

    /**
     * Human readable name of the task that should be run to correct any errors detected by this task.
     */
    @Input
    abstract Property<String> getGenerateTask()

    @Inject
    protected FileLookup getFileLookup() {
        throw new UnsupportedOperationException();
    }

    @Internal
    ConfigurableFileCollection getExpectedContentsFiles() {
        project.files(getExpectedContents().get())
    }

    void actualContents(Action<ActualContents> action) {
        action.execute(actualContentsHolder)

        if (!existingFiles.isEmpty()) {
            throw new IllegalStateException("Can't be run twice")
        }
        def ignoreSpec = actualContentsHolder.ignoreInActual.getAsSpec()
        project.fileTree(actualContentsHolder.actualContents).visit { FileVisitDetails details ->
            if (ignoreSpec.isSatisfiedBy(details)) {
                return;
            }
            if (details.isDirectory()) {
                return;
            }
            existingFiles.add(details.file);
        }
        inputs.files(existingFiles)
    }

    @TaskAction
    void diff() {
        def resolver = getFileLookup().getFileResolver(actualContentsHolder.actualContents.asFile)
        // for each file in the generated go output, make sure it exists and matches contents
        Set<Path> changed = []
        Set<Path> missing = []

        expectedContentsFiles.asFileTree.visit { FileVisitDetails details ->
            if (details.isDirectory()) {
                return;
            }

            // note the relative path of each generated file
            def pathString = details.relativePath.pathString

            File sourceFile = resolver.resolve(pathString)
            // if the file does not exist in our source dir, add an error
            if (!sourceFile.exists()) {
                missing.add(sourceFile.toPath())
            } else {
                // remove this from the "existing" collection so we can detect extra files later
                existingFiles.remove(sourceFile)

                // verify that the contents match
                if (sourceFile.text != details.file.text) {
                    changed.add(sourceFile.toPath())
                }
            }
        }
        if (!changed.isEmpty() || !missing.isEmpty() || !existingFiles.isEmpty()) {
            logger.error("Sources do not match expected files:")
            changed.each {
                logger.error("File has changes: $it")
            }
            missing.each {
                logger.error("File is missing: $it")
            }
            existingFiles.each {
                logger.error("File should not exist: $it")
            }
            if (generateTask.isPresent()) {
                throw new RuntimeException("Sources do not match expected, re-run ${generateTask.get()}")
            } else {
                throw new RuntimeException("Sources do not match expected");
            }
        }
    }
}
