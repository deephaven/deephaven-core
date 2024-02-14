package io.deephaven.tools.docker

import com.bmuschko.gradle.docker.tasks.AbstractDockerRemoteApiTask
import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.command.CopyArchiveFromContainerCmd
import com.github.dockerjava.api.command.CreateContainerCmd
import com.github.dockerjava.api.command.CreateContainerResponse
import com.github.dockerjava.api.command.LogContainerCmd
import com.github.dockerjava.api.command.RemoveContainerCmd
import com.github.dockerjava.api.command.StartContainerCmd
import com.github.dockerjava.api.command.WaitContainerCmd
import com.github.dockerjava.api.model.Frame
import com.github.dockerjava.api.model.WaitResponse
import groovy.io.FileType
import org.gradle.api.GradleException
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Nested
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputDirectory

import javax.inject.Inject
import java.util.concurrent.TimeUnit

/**
 * Combined work of DockerCreateContainer, DockerStartContainer, and DockerLogsContainer, such that both
 * inputs and outputs are declared. Only specific inputs are exposed, in part because they might be
 * incompatible with gradle's up-to-date checking, and in part because they haven't been necessary.
 */
class CombinedDockerRunTask extends AbstractDockerRemoteApiTask {
    @Input
    final Property<String> imageId = project.objects.property(String)

    @Nested
    final DockerCreateContainer.HostConfig hostConfig

    @Input
    @Optional
    final ListProperty<String> entrypoint = project.objects.listProperty(String)

    @Input
    final Property<Integer> awaitStatusTimeoutSeconds = project.objects.property(Integer)

    @Input
    final Property<String> remotePath = project.objects.property(String)

    @OutputDirectory
    final DirectoryProperty outputDir = project.objects.directoryProperty()

    @Inject
    CombinedDockerRunTask(ObjectFactory objectFactory) {
        hostConfig = objectFactory.newInstance(DockerCreateContainer.HostConfig, objectFactory)
    }

    @Override
    void runRemoteCommand() {
        String containerId;
        try {
            // Create the container
            CreateContainerCmd createContainerCmd = dockerClient.createContainerCmd(imageId.get())

            //inlined setContainerCommandConfig(containerCommand)
            if (hostConfig.network.getOrNull()) {
                createContainerCmd.hostConfig.withNetworkMode(hostConfig.network.get())
            }
            if (entrypoint.getOrNull()) {
                createContainerCmd.withEntrypoint(entrypoint.get())
            }

            CreateContainerResponse container = createContainerCmd.exec()
            logger.quiet "Created container with ID '$container.id'."
            containerId = container.id

            // If there is an entrypoint (else no need to start, we just copy out of existing one
            if (entrypoint.getOrNull()) {
                // Start the container and wait for finish
                StartContainerCmd startContainerCmd = dockerClient.startContainerCmd(containerId)
                startContainerCmd.exec()

                WaitContainerCmd waitCommand = dockerClient.waitContainerCmd(containerId)

                ResultCallback<WaitResponse> callback = waitCommand.start()
                def failedMessage = null;
                try {
                    def exitCode = callback.awaitStatusCode(awaitStatusTimeoutSeconds.get(), TimeUnit.SECONDS)
                    logger.quiet "Container exited with code ${exitCode}"
                    if (exitCode != 0) {
                        failedMessage = "Command '${entrypoint.get()}' failed with status code ${exitCode}";
                    }
                } catch (Exception exception) {
                    if (exception.message.contains('timeout')) {
                        failedMessage = "Command '${entrypoint.get()}' timed out after ${awaitStatusTimeoutSeconds.get()} seconds"
                    } else {
                        failedMessage = "Command '${entrypoint.get()}' failed: " + exception.message
                    }
                }

                // If the task failed, write all logs
                if (failedMessage) {
                    LogContainerCmd logCommand = dockerClient.logContainerCmd(containerId)
                    logCommand.withContainerId(containerId)
                    logCommand.withStdErr(true)
                    logCommand.withStdOut(true)

                    logCommand.exec(createCallback())?.awaitCompletion(10, TimeUnit.SECONDS)

                    throw new GradleException("${failedMessage}, check logs for details")
                }
            }
        } finally {
            if (containerId == null) {
                return;
            }
            // Copy output to internal output directory
            CopyArchiveFromContainerCmd copyCommand = dockerClient.copyArchiveFromContainerCmd(containerId, remotePath.get())
            logger.quiet "Copying '${remotePath.get()}' from container with ID '${containerId}' to '${outputDir.get()}'."
            InputStream tarStream
            try {
                tarStream = copyCommand.exec()

                def hostDestination = outputDir.get().asFile

                hostDestination.deleteDir()
                copyFile(tarStream, hostDestination)
            } finally {
                tarStream?.close()
            }

            RemoveContainerCmd removeCommand = dockerClient.removeContainerCmd(containerId)
            removeCommand.withRemoveVolumes(true)
            removeCommand.withForce(true)
            logger.quiet "Removing container with ID '${containerId}'."
            try {
                removeCommand.exec()
            } catch (Exception ignored) {
                // Failure is possible if some other part of the task failed
            }
        }
    }

    private ResultCallback.Adapter<Frame> createCallback() {
        return new ResultCallback.Adapter<Frame>() {
            @Override
            void onNext(Frame frame) {
                // delegate to avoid an apparent groovy bytecode error
                writeLog(frame)
            }
        }
    }
    void writeLog(Frame frame) {
        try {
            switch (frame.streamType as String) {
                case 'STDOUT':
                case 'RAW':
                    logger.quiet(new String(frame.payload).replaceFirst(/\s+$/, ''))
                    break
                case 'STDERR':
                    logger.error(new String(frame.payload).replaceFirst(/\s+$/, ''))
                    break
            }
        } catch(Exception e) {
            logger.error('Failed to handle frame', e)
        }
    }

    /**
     * Copy regular file or directory from container to host
     */
    private void copyFile(InputStream tarStream, File hostDestination) {
        def tempDestination
        try {
            tempDestination = untarStream(tarStream)
            /*
                At this juncture we have 3 possibilities:
                    1.) 0 files were found in which case we do nothing
                    2.) 1 regular file was found
                    3.) N regular files (and possibly directories) were found
            */
            def fileCount = 0
            tempDestination.eachFileRecurse(FileType.FILES) { fileCount++ }
            if (fileCount == 0) {
                logger.quiet "Nothing to copy."
            } else if (fileCount == 1) {
                copySingleFile(hostDestination, tempDestination)
            } else {
                copyMultipleFiles(hostDestination, tempDestination)
            }
        } finally {
            if(!tempDestination?.deleteDir()) {
                throw new GradleException("Failed deleting directory at ${tempDestination.path}")
            }
        }
    }
    /**
     * Unpack tar stream into generated directory relative to $buildDir
     */
    private File untarStream(InputStream tarStream) {
        def tempFile
        def outputDirectory
        try {
            // Write tar to temp location since we are exploding it anyway
            tempFile = File.createTempFile(UUID.randomUUID().toString(), ".tar")
            tempFile.withOutputStream { it << tarStream }

            // We are not allowed to rename tempDir's created in OS temp directory (as
            // we do further downstream) which is why we are creating it relative to
            // build directory
            outputDirectory = project.layout.buildDirectory.dir(UUID.randomUUID().toString()).get().asFile
            if(!outputDirectory.mkdirs()) {
                throw new GradleException("Failed creating directory at ${outputDirectory.path}")
            }

            project.copy {
                into outputDirectory
                from project.tarTree(tempFile)
            }
            return outputDirectory

        } finally {
            if(!tempFile.delete()) {
                throw new GradleException("Failed deleting previously existing file at ${tempFile.path}")
            }
        }
    }

    /**
     * Copy regular file inside tempDestination to, or into, hostDestination
     */
    private static void copySingleFile(File hostDestination, File tempDestination) {

        // ensure regular file does not exist as we don't want clobbering
        if (hostDestination.exists() && !hostDestination.isDirectory() && !hostDestination.delete()) {
            throw new GradleException("Failed deleting file at ${hostDestination.path}")
        }

        // create parent files of hostPath should they not exist
        if (!hostDestination.exists() && !hostDestination.parentFile.exists() && !hostDestination.parentFile.mkdirs()) {
            throw new GradleException("Failed creating parent directory for ${hostDestination.path}")
        }

        def parentDirectory = hostDestination.isDirectory() ? hostDestination : hostDestination.parentFile
        def fileName = hostDestination.isDirectory() ? tempDestination.listFiles().last().name : hostDestination.name

        def destination = new File(parentDirectory, fileName)
        if (!tempDestination.listFiles().last().renameTo(destination)) {
            throw new GradleException("Failed renaming file ${tempDestination.path} to ${destination.path}")
        }
    }

    /**
     * Copy files inside tempDestination into hostDestination
     */
    private static void copyMultipleFiles(File hostDestination, File tempDestination) {

        // Flatten single top-level directory to behave more like docker. Basically
        // we are turning this:
        //
        //     /<requested-host-dir>/base-directory/actual-files-start-here
        //
        // into this:
        //
        //     /<requested-host-dir>/actual-files-start-here
        //
        // gradle does not currently offer any mechanism to do this which
        // is why we have to do the following gymnastics
        if (tempDestination.listFiles().size() == 1) {
            def dirToFlatten = tempDestination.listFiles().last()
            def dirToFlattenParent = tempDestination.listFiles().last().parentFile
            def flatDir = new File(dirToFlattenParent, UUID.randomUUID().toString())

            // rename origin to escape potential clobbering
            if(!dirToFlatten.renameTo(flatDir)) {
                throw new GradleException("Failed renaming file ${dirToFlatten.path} to ${flatDir.path}")
            }

            // rename files 1 level higher
            flatDir.listFiles().each {
                def movedFile = new File(dirToFlattenParent, it.name)
                if (!it.renameTo(movedFile)) {
                    throw new GradleException("Failed renaming file ${it.path} to ${movedFile.path}")
                }
            }

            if (!flatDir.deleteDir()) {
                throw new GradleException("Failed deleting directory at ${flatDir.path}")
            }
        }

        // delete regular file should it exist
        if (hostDestination.exists() && !hostDestination.isDirectory())
            if(!hostDestination.delete())
                throw new GradleException("Failed deleting file at ${hostDestination.path}")

        // If directory already exists, rename each file into said directory, otherwise rename entire directory.
        if (hostDestination.exists()) {
            def parentName = tempDestination.name
            tempDestination.listFiles().each {
                def originPath = it.absolutePath
                def index = originPath.lastIndexOf(parentName) + parentName.length()
                def relativePath = originPath.substring(index, originPath.length())
                def destFile = new File("${hostDestination.path}/${relativePath}")
                if (!it.renameTo(destFile)) {
                    throw new GradleException("Failed renaming file ${it.path} to ${destFile.path}")
                }
            }
        } else if (!tempDestination.renameTo(hostDestination)) {
            throw new GradleException("Failed renaming file ${tempDestination.path} to ${hostDestination.path}")
        }
    }

}
