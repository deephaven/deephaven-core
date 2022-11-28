package io.deephaven.tools.docker

import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.FileVisitDetails
import org.gradle.api.internal.file.FileLookup
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
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
    // This is an Object because Gradle doesn't define an interface for getAsFileTree(), so we
    // will resolve it when we execute the task. This allows us to read from various sources,
    // such as configurations.
    @InputFiles
    abstract Property<Object> getExpectedContents()
    // In contrast, this is assumed to be a source directory, to easily allow some Sync action
    // to easily be the "fix this mistake" counterpart to this task
    @InputDirectory
    abstract DirectoryProperty getActualContents()

    private final PatternSet ignoreInActual = new PatternSet();

    /**
     * Marks some contents as not needing to be diff'd, like Sync's "preserve". Counter-intuitively,
     * this usually requires an "exclude" to specify only a specific subdirectory should be diff'd.
     */
    DiffTask ignore(Action<? super PatternFilterable> action) {
        action.execute(this.ignoreInActual);
        return this;
    }

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

    @TaskAction
    void diff() {
        def resolver = getFileLookup().getFileResolver(getActualContents().asFile.get())
        // for each file in the generated go output, make sure it exists and matches contents
        Set<Path> changed = []
        Set<Path> missing = []
        // build this list before we traverse, then remove as we go, to represent files that shouldn't exist
        Set<Path> existingFiles = []

        def ignoreSpec = ignoreInActual.getAsSpec()
        getActualContents().asFileTree.visit { FileVisitDetails details ->
            if (ignoreSpec.isSatisfiedBy(details)) {
                return;
            }
            if (details.isDirectory()) {
                return;
            }
            existingFiles.add(details.file.toPath());
        }

        expectedContentsFiles.asFileTree.visit { FileVisitDetails details ->
            if (details.isDirectory()) {
                return;
            }

            // note the relative path of each generated file
            def pathString = details.relativePath.pathString

            def sourceFile = resolver.resolve(pathString)
            // if the file does not exist in our source dir, add an error
            if (!sourceFile.exists()) {
                missing.add(sourceFile.toPath())
            } else {
                // remove this from the "existing" collection so we can detect extra files later
                existingFiles.remove(sourceFile.toPath())

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
