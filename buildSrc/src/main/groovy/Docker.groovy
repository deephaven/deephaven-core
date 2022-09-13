import com.bmuschko.gradle.docker.tasks.container.DockerCopyFileFromContainer
import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerLogsContainer
import com.bmuschko.gradle.docker.tasks.container.DockerRemoveContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerWaitContainer
import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import com.bmuschko.gradle.docker.tasks.image.DockerInspectImage
import com.bmuschko.gradle.docker.tasks.image.DockerPullImage
import com.bmuschko.gradle.docker.tasks.image.DockerRemoveImage
import com.bmuschko.gradle.docker.tasks.image.Dockerfile
import com.github.dockerjava.api.command.InspectImageResponse
import com.github.dockerjava.api.exception.DockerException
import groovy.transform.CompileStatic
import io.deephaven.tools.docker.Architecture
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Sync
import org.gradle.api.tasks.TaskProvider
import org.gradle.util.ConfigureUtil

/**
 * Tools to make some common tasks in docker easier to use in gradle
 */
@CompileStatic
class Docker {
    private static final String LOCAL_BUILD_TAG = 'local-build'

    /**
     * Helper method to make sure we rebuild the image if it is out of date. At
     * present, is only applicable if there are tags to set on the image
     * Usage:
     * <pre>
     *     DockerBuildImage myTask = ...
     *     task.upToDateWhen { Docker.isImageUpToDate(myTask) }
     * </pre>
     *
     * @link https://github.com/bmuschko/gradle-docker-plugin/issues/1008
     * @param t the docker build image task to check
     * @return
     */
    static boolean isImageUpToDate(DockerBuildImage t) {
        File file = t.imageIdFile.get().asFile
        if (file.exists()) {
            try {
                // get the last imageId we used
                def fileImageId = file.text
                // check if that image still exists
                for (String image : t.images.get()) {
                    def inspect = t.getDockerClient().inspectImageCmd(image).exec();
                    // see if that image is tagged the way we expectif not, re-run
                    def sha = inspect.id.substring("sha:256".length());
                    if (sha != fileImageId && !sha.startsWith(fileImageId)) {
                        return false;
                    }
                }
                return true;
            } catch (DockerException e) {
                // if we fail, it must not have existed, re-run the task
                return false
            }
        }
        // the imageIdFile didn't exist, so we definitely need to build
        return false
    }

    /**
     * DSL object to describe a docker task
     */
    static class DockerTaskConfig {

        private Action<? super CopySpec> copyIn;
        private Action<? super Sync> copyOut;
        private File dockerfileFile;
        private Action<? super Dockerfile> dockerfileAction;
        private TaskDependencies containerDependencies = new TaskDependencies();

        /**
         * Files that need to be copied in to the image.
         */
        DockerTaskConfig copyIn(Action<? super CopySpec> action) {
            copyIn = action;
            return this;
        }
        /**
         * Files that need to be copied in to the image.
         */
        DockerTaskConfig copyIn(Closure closure) {
            return copyIn(ConfigureUtil.configureUsing(closure))
        }

        /**
         * Resulting files to copy out from the containerOutPath.
         */
        DockerTaskConfig copyOut(Action<? super Sync> action) {
            copyOut = action;
            return this;
        }
        /**
         * Resulting files to copy out from the containerOutPath.
         */
        DockerTaskConfig copyOut(Closure closure) {
            return copyOut(ConfigureUtil.configureUsing(closure))
        }

        /**
         * Dockerfile to use. If not set, it is assumed that a dockerfile will be included in copyIn.
         */
        DockerTaskConfig dockerfile(File dockerfile) {
            this.dockerfileFile = dockerfile;
            return this;
        }
        /**
         * Dockerfile to use. If not set, it is assumed that a dockerfile will be included in copyIn.
         */
        DockerTaskConfig dockerfile(Action<? super Dockerfile> action) {
            this.dockerfileAction = action;
            return this;
        }
        /**
         * Dockerfile to use. If not set, it is assumed that a dockerfile will be included in copyIn.
         */
        DockerTaskConfig dockerfile(Closure closure) {
            dockerfile(ConfigureUtil.configureUsing(closure));
        }

        /**
         * Tag to apply to the created image. Defaults to "deephaven/" followed by the task name.
         */
        String imageName;

        /**
         * Name of the docker network which the container should be attached to.
         */
        String network;

        /**
         * Path inside the created docker container that contains the output to be copied out as part of this task
         */
        String containerOutPath = '/out'

        /**
         * List of any containers, the tasks that create them.
         */
        List<Task> parentContainers = []

        /**
         * Optional command to run whenever the task is invoked, otherwise the image's contents will be used
         * as-is.
         */
        List<String> entrypoint;

        /**
         * Optional build arguments
         */
        Map<String, String> buildArgs;

        /**
         * Optional platform
         */
        String platform;

        /**
         * Optional set of environment variables to set on the container.
         */
        Map<String, String> envVars;

        /**
         * Logs are always printed from the build task when it runs, but entrypoint logs are only printed
         * when it fails. Set this flag to always show logs, even when entrypoint is successful.
         */
        boolean showLogsOnSuccess;
    }
    /**
     * Describes relationships between this set of tasks and other external tasks.
     */
    static class TaskDependencies {
        /**
         * Indicates tasks that must have been successfully completed before the container can start.
         */
        Object dependsOn;
        /**
         * Indicates tasks that should run after the container has stopped.
         */
        Object finalizedBy;
    }

    private static void validateImageName(String imageName) {
        if (!imageName.endsWith(":${LOCAL_BUILD_TAG}")) {
            throw new IllegalArgumentException("imageName '${imageName}' is invalid, it must be tagged with '${LOCAL_BUILD_TAG}'")
        }
    }

    /**
     * Creates a task to run docker to do some work in a container rather than in the hosted environment.
     *
     * @param project the project this is being invoked on
     * @param taskName the name to call the new task
     * @param closure wrapper to configure a DockerTaskConfig instance
     * @return a task provider for the Sync task that will produce the requested output
     */
    static TaskProvider<? extends Task> registerDockerTask(Project project, String taskName, Closure closure) {
        return registerDockerTask(project, taskName, ConfigureUtil.configureUsing(closure))
    }

    /**
     * Creates a task to run docker to do some work in a container rather than in the hosted environment.
     *
     * @param project the project this is being invoked on
     * @param taskName the name to call the new task
     * @param action wrapper to configure a DockerTaskConfig instance
     * @return a task provider for the Sync task that will produce the requested output
     */
    static TaskProvider<? extends Task> registerDockerTask(Project project, String taskName, Action<? super DockerTaskConfig> action) {
        // create instance, assign defaults
        DockerTaskConfig cfg = new DockerTaskConfig();
        // Change taskNameFormat into deephaven/task-name-format
        cfg.imageName = "deephaven/${taskName.replaceAll(/\B[A-Z]/, /-$0/).toLowerCase()}:${LOCAL_BUILD_TAG}"

        // ask for more configuration
        action.execute(cfg)

        validateImageName(cfg.imageName)

        String dockerContainerName = "$taskName-container-${UUID.randomUUID()}"
        String dockerCopyLocation = "${project.buildDir}/$taskName-tmp-copy"
        // This directory is always marked as the inputs for the subsequent DockerBuildImage task, setting it here to
        // be explicit and scoped to this task
        File dockerWorkspaceContents = project.file("${project.buildDir}/$taskName-docker");

        // If needed, make a Dockerfile from config
        TaskProvider<Dockerfile> dockerfileTask

        if (cfg.dockerfileAction && cfg.dockerfileFile) {
            throw new IllegalStateException("Cannot specify dockerfile as both path and closure")
        }

        if (cfg.dockerfileAction) {
            dockerfileTask = project.tasks.register("${taskName}Dockerfile", Dockerfile) { dockerfile ->
                cfg.dockerfileAction.execute(dockerfile)
                dockerfile.destFile.set new File(dockerWorkspaceContents.path + 'file', 'Dockerfile')
            }
        }

        // Copy the requested files into build/docker
        def prepareDocker = project.tasks.register("${taskName}PrepareDocker", Sync) { sync ->
            // First, apply the provided spec
            cfg.copyIn.execute(sync)

            // Then, make sure we write into our docker dir
            sync.into dockerWorkspaceContents

            if (cfg.dockerfileFile) {
                sync.from(cfg.dockerfileFile) { CopySpec dockerfileCopy ->
                    dockerfileCopy.include(cfg.dockerfileFile.name)
                }
            } else if (cfg.dockerfileAction) {
                sync.from(dockerfileTask.get().outputs.files) { CopySpec dockerfileCopy ->
                    dockerfileCopy.include('Dockerfile')
                }
            }
        }

        TaskProvider<DockerBuildImage> makeImage = registerDockerImage(project, "${taskName}MakeImage") { DockerBuildImage buildImage ->
            buildImage.with {
                // assign our own workspace dir
                inputDir.set dockerWorkspaceContents
                // set it as inputs anyway, in case this changes, as it is also an implicit dependsOn
                inputs.files prepareDocker.get().outputs.files

                // specify that we rely on any parent container's outputs
                // this is superior to using dependsOn, since it means we will re-run correctly when the upstream image
                // is updated
                inputs.files cfg.parentContainers.each { t -> t.outputs.files }


                // specify tag, if provided
                if (cfg.imageName) {
                    images.add(cfg.imageName)
                }

                // add build arguments, if provided
                if (cfg.buildArgs) {
                    buildArgs.putAll(cfg.buildArgs)
                }

                // the platform, if provided
                if (cfg.platform) {
                    platform.set(cfg.platform)
                }
            }
        }

        // Create a new container from the image above, as a workaround to extract the output from the dockerfile's
        // build steps
        TaskProvider<DockerCreateContainer> createContainer = project.tasks.register("${taskName}CreateContainer", DockerCreateContainer) { createContainer ->
            createContainer.with {
                // this could probably be simplified to a dependsOn, since we already use its imageId as an input
                inputs.files makeImage.get().outputs.files

                if (cfg.entrypoint) {
                    // if provided, set a run command that we'll use each time it starts
                    entrypoint.set(cfg.entrypoint)
                }

                if (cfg.network) {
                    hostConfig.network.set(cfg.network)
                }

                if (cfg.containerDependencies.dependsOn) {
                    dependsOn(cfg.containerDependencies.dependsOn)
                }
                if (cfg.envVars) {
                    createContainer.envVars.putAll cfg.envVars
                }

                targetImageId makeImage.get().getImageId()
                containerName.set(dockerContainerName)
            }
        }

        // Remove container after its contents have been extracted. Note that this could fail to run if the
        // worker is killed, since the container name is generated fresh each time
        TaskProvider<DockerRemoveContainer> removeContainer = project.tasks.register("${taskName}RemoveContainer", DockerRemoveContainer) { removeContainer ->
            removeContainer.with {
                //TODO wire this up to not even run if the container doesn't exist
                dependsOn createContainer
                targetContainerId dockerContainerName

                if (cfg.containerDependencies.finalizedBy) {
                    finalizedBy(cfg.containerDependencies.finalizedBy)
                }

                onError { t ->
                    // ignore, container might not exist
                }
            }
        }

        // Optionally lets us run the container each invocation with a command (such as for tests). This will
        // only be used if cfg.command is set
        TaskProvider<DockerStartContainer> startContainer = project.tasks.register("${taskName}StartContainer", DockerStartContainer) { startContainer ->
            startContainer.with {
                startContainer.dependsOn createContainer
                containerId.set(dockerContainerName)
            }
        }
        TaskProvider<DockerWaitContainer> containerFinished = project.tasks.register("${taskName}WaitContainer", DockerWaitContainer) { waitContainer ->
            waitContainer.with {
                dependsOn startContainer
                containerId.set(dockerContainerName)
            }
        }
        TaskProvider<DockerLogsContainer> containerLogs = project.tasks.register("${taskName}LogsContainer", DockerLogsContainer) { logsContainer ->
            logsContainer.with {
                containerId.set(dockerContainerName)
                dependsOn containerFinished
                onlyIf {
                    cfg.entrypoint && (containerFinished.get().exitCode != 0 || cfg.showLogsOnSuccess)
                }
            }
        }

        if (!cfg.copyOut) {
            // Make a wrap-up task to clean up the task work, wait until things are finished, since we have nothing to copy out
            return project.tasks.register(taskName) { task ->
                task.with {
                    if (cfg.entrypoint) {
                        dependsOn containerFinished, containerLogs
                        doLast {
                            // There was an entrypoint specified, if the command was not successful kill the build once
                            // we're done copying output. Note that this means the output is actually thrown away (aside
                            // from being writen to the log this build)
                            if (containerFinished.get().exitCode != 0) {
                                throw new GradleException("Command '${cfg.entrypoint.join(' ')}' failed with exit code ${containerFinished.get().exitCode}, check logs for details")
                            }
                            logger.quiet('Entrypoint has been executed, but no output is copied out.')
                        }
                    } else {
                        dependsOn createContainer
                    }
                    finalizedBy removeContainer

                    // We need to declare some output so that other tasks can correctly depend on this. Whether or not
                    // there is an entrypoint, the last accessible output is the build image, so declare that
                    outputs.files makeImage.get().outputs.files
                }
            }
        }

        // Copy the results from the build out of the container, so the sync task can make it available
        TaskProvider<DockerCopyFileFromContainer> copyGenerated = project.tasks.register("${taskName}CopyGeneratedOutput", DockerCopyFileFromContainer) { copy ->
            copy.with {
                if (cfg.entrypoint) {
                    dependsOn containerFinished, containerLogs
                } else {
                    dependsOn createContainer
                }

                // once we're done copying output, delete the container
                finalizedBy removeContainer

                // specify that we don't need to re-run if the imageid didn't change
                inputs.property('imageId', makeImage.get().getImageId())
                outputs.dir(dockerCopyLocation)

                targetContainerId createContainer.get().getContainerId()

                remotePath.set(cfg.containerOutPath)
                hostPath.set(dockerCopyLocation)

                doFirst {
                    // we must manually delete this first, since docker cp will error if trying to overwrite
                    project.delete(dockerCopyLocation)
                }
                doLast {
                    if (cfg.entrypoint && containerFinished.get().exitCode != 0) {
                        // The entrypoing existed and failed. Since we rely on the Sync task (right after this one,
                        // returned from this method) to communicate that failure, we need to create some noop file
                        // if no other file was copied.

                        // since we rely on the Sync task to communicate failure (as it is returned), we need to
                        // make some noop file if no other files were copied
                        def copyLoc = new File(dockerCopyLocation)
                        if (!copyLoc.exists() || copyLoc.list().length == 0) {
                            copyLoc.mkdirs()
                            // make a new file, ensure it is always fresh
                            def nonce = new File(copyLoc, 'no-contents.txt')
                            nonce.createNewFile()
                            nonce.write("Empty marker file, since no output was copied from the entrypoint's failure (${new Date()})")
                        }

                    }
                }
            }
        }

        // Sync the results to where the caller requested us to put them
        return project.tasks.register(taskName, Sync) { sync ->
            sync.with {
                dependsOn copyGenerated

                if (cfg.entrypoint) {
                    doLast {
                        // there was an entrypoint specified, if the command was not successful kill the build once
                        // we're done copying output
                        if (containerFinished.get().exitCode != 0) {
                            throw new GradleException("Command '${cfg.entrypoint.join(' ')}' failed with exit code ${containerFinished.get().exitCode}, check logs for details")
                        }
                    }
                }

                // run the provided closure first
                cfg.copyOut.execute(sync)

                // then set the from location
                from dockerCopyLocation
            }
        }
    }

    static void checkValidTwoPhase(DockerBuildImage buildImage) {
        if (buildImage.target.isPresent()) {
            throw new IllegalArgumentException("Two phase build should not be setting target, is '${buildImage.target.get()}'")
        }
        if (buildImage.images.isPresent() && !buildImage.images.get().isEmpty()) {
            throw new IllegalArgumentException("Two phase build should not be setting images, is '${buildImage.images.get()}'")
        }
    }

    static TaskProvider<? extends DockerBuildImage> registerDockerTwoPhaseImage(Project project, String baseName, String intermediate, Closure closure) {
        return registerDockerTwoPhaseImage(project, baseName, intermediate, ConfigureUtil.configureUsing(closure))
    }

    static TaskProvider<? extends DockerBuildImage> registerDockerTwoPhaseImage(Project project, String baseName, String intermediate, Action<? super DockerBuildImage> action) {
        // Explicitly target and tag the intermediate task; otherwise, docker will leave it unnamed, and we won't be
        // able to clean it up.
        def intermediateTask = registerDockerImage(project, "buildDocker-${baseName}-${intermediate}") { DockerBuildImage buildImage ->
            action.execute(buildImage)
            checkValidTwoPhase(buildImage)
            buildImage.target.set(intermediate)
            buildImage.images.add("deephaven/${baseName}-${intermediate}:local-build".toString())
        }

        return registerDockerImage(project, "buildDocker-${baseName}") { DockerBuildImage buildImage ->
            action.execute(buildImage)
            checkValidTwoPhase(buildImage)
            buildImage.dependsOn(intermediateTask)
            buildImage.images.add("deephaven/${baseName}:local-build".toString())
        }
    }

    static TaskProvider<? extends DockerBuildImage> registerDockerImage(Project project, String taskName, Closure closure) {
        return registerDockerImage(project, taskName, ConfigureUtil.configureUsing(closure))
    }

    static TaskProvider<? extends DockerBuildImage> registerDockerImage(Project project, String taskName, Action<? super DockerBuildImage> action) {
        // Produce a docker image from the copied inputs and provided dockerfile, and tag it
        TaskProvider<DockerBuildImage> makeImage = project.tasks.register(taskName, DockerBuildImage) { buildImage ->
            action.execute(buildImage)
            if (!buildImage.platform.isPresent()) {
                def targetArch = Architecture.targetArchitecture(project).toString()
                buildImage.platform.set "linux/${targetArch}".toString()
                // Use the same environment variable that buildkit uses
                buildImage.buildArgs.put('TARGETARCH', targetArch)
            }
            if (buildImage.images) {
                buildImage.images.get().forEach { String imageName -> validateImageName(imageName) }

                // apply fix, since tags don't work properly
                buildImage.outputs.upToDateWhen {
                    isImageUpToDate(buildImage)
                }
            }
        }

        // Enabling removing the image as part of clean task
        TaskProvider<DockerRemoveImage> removeImage = project.tasks.register("${taskName}Clean", DockerRemoveImage) { removeImage ->
            removeImage.with {
                Set<String> images = makeImage.get().images.get();
                if (images.isEmpty()) {
                    // don't bother to run if no tag was set, it'll get gc'd automatically at some point
                    onlyIf { false }
                } else {
                    // We assume exactly one tag set
                    targetImageId images.iterator().next()
                    onError { t ->
                        // ignore, the image might not exist
                    }
                }
            }
        }
        project.tasks.findByName('clean').dependsOn removeImage

        return makeImage;
    }

    static TaskProvider<? extends Task> buildPyWheel(Project project, String taskName, String imgName, String sourcePath) {
        project.evaluationDependsOn(registryProject('python'))
        return registerDockerTask(project, taskName) { DockerTaskConfig config ->
            config.copyIn { Sync sync ->
                sync.from(sourcePath) { CopySpec copySpec ->
                    copySpec.exclude 'build', 'dist'
                    copySpec.into 'src'
                }
            }
            config.imageName = "${imgName}:local-build"
            config.dockerfile { Dockerfile action ->
                // set up the container, env vars - things that aren't likely to change
                action.from 'deephaven/python:local-build as sources'
                action.arg 'DEEPHAVEN_VERSION'
                action.environmentVariable 'DEEPHAVEN_VERSION', project.version.toString()
                action.workingDir '/usr/src/app'
                action.copyFile '/src', '.'
                action.from 'sources as build'
                action.runCommand '''set -eux; \\
                      test -n "${DEEPHAVEN_VERSION}";\\
                      python setup.py bdist_wheel'''
            }
            config.parentContainers = [ registryTask(project, 'python') ]
            config.containerOutPath='/usr/src/app/dist'
            config.copyOut { Sync sync ->
                sync.into "build/wheel${taskName}"
            }
        }
    }


    static TaskProvider<? extends DockerBuildImage> registryRegister(Project project) {

        String imageName = project.property('deephaven.registry.imageName')
        String imageId = project.property('deephaven.registry.imageId')
        boolean ignoreOutOfDate = project.hasProperty('deephaven.registry.ignoreOutOfDate') ?
                'true' == project.property('deephaven.registry.ignoreOutOfDate') :
                false

        project.tasks.register('showImageId') {
            it.doLast {
                println(imageId)
            }
        }

        def pullImage = project.tasks.register('pullImage', DockerPullImage) { pull ->
            pull.group = 'Docker Registry'
            pull.description = "Release management task: Pulls '${imageName}'"
            pull.image.set imageName
        }

        def bumpImage = project.tasks.register('bumpImage', DockerInspectImage) {inspect ->
            inspect.group = 'Docker Registry'
            inspect.description = "Release management task: Updates the gradle.properties file for '${imageName}'"
            inspect.imageId.set imageName
            inspect.mustRunAfter pullImage
            inspect.onNext { InspectImageResponse message ->
                if (message.repoDigests.isEmpty()) {
                    throw new RuntimeException("Image '${imageName}' from the (local) repository does not have a repo digest. " +
                            "This is an unexpected situation, unless you are manually building the image.")
                }
                if (message.repoDigests.size() > 1) {
                    throw new RuntimeException("Unable to bump the imageId for '${imageName}' since there are mulitple digests: '${message.repoDigests}'.\n" +
                            "Please update the property `deephaven.registry.imageId` in the file '${project.projectDir}/gradle.properties' manually.")
                }
                def repoDigest = message.repoDigests.get(0)

                if (repoDigest != imageId) {
                    new File(project.projectDir, 'gradle.properties').text =
                            "io.deephaven.project.ProjectType=DOCKER_REGISTRY\n" +
                                    "deephaven.registry.imageName=${imageName}\n" +
                                    "deephaven.registry.imageId=${repoDigest}\n"
                    inspect.logger.quiet("Updated imageId for '${imageName}' to '${repoDigest}' from '${imageId}'.")
                } else {
                    inspect.logger.quiet("ImageId for '${imageName}' already up-to-date.")
                }
            }
        }

        project.tasks.register('compareImage', DockerInspectImage) {inspect ->
            inspect.group = 'Docker Registry'
            inspect.description = "Release management task: Compares the (local) repository contents for '${imageName}' against source-control contents."
            inspect.imageId.set imageName
            inspect.mustRunAfter pullImage
            inspect.onNext { InspectImageResponse message ->
                if (message.repoDigests.isEmpty()) {
                    throw new RuntimeException("Image '${imageName}' from the (local) repository does not have a repo digest. " +
                            "This is an unexpected situation, unless you are manually building the image.")
                }
                if (!(imageId in message.repoDigests)) {
                    String text = "The imageId for '${imageName}' appears to be out-of-sync with the (local) repository. " +
                            "Possible repo digests are '${message.repoDigests}'.\n" +
                            "Consider running one of the following, and retrying the compare, to see if the issue persists:\n" +
                            "\t`./gradlew ${project.name}:${pullImage.get().name}`, or\n" +
                            "\t`docker pull ${imageName}`\n\n" +
                            "If the image is still out-of-sync, it's likely that there is a new release for '${imageName}'.\n" +
                            "You may run:\n" +
                            "\t`./gradlew ${project.name}:${bumpImage.get().name}`, or\n" +
                            "\tmanually update '${project.projectDir}/gradle.properties' to bring the build logic up-to-date."
                    if (ignoreOutOfDate) {
                        inspect.logger.warn(text)
                    } else {
                        throw new RuntimeException(text)
                    }
                }
            }
            inspect.onError {error ->
                if (error.message.contains('no such image')) {
                    throw new RuntimeException("Unable to find the image '${imageName}' in the (local) repository.\n" +
                            "Consider running one of the following:\n" +
                            "\t`./gradlew ${project.name}:${pullImage.get().name}`, or\n" +
                            "\t`docker pull ${imageName}`, or\n" +
                            "\t`docker tag <source> ${imageName}`")
                }
                throw error
            }
        }

        def dockerfile = project.tasks.register('dockerfile', Dockerfile) { dockerfile ->
            dockerfile.description = "Internal task: creates a dockerfile, to be (built) tagged as 'deephaven/${project.projectDir.name}:local-build'."
            dockerfile.from(imageId)
        }

        project.tasks.register('createCraneTagScript', Sync) {
            it.description = "Release task: Creates a crane tag script for '${imageName}'"
            it.from("${project.rootDir}/buildSrc/src/crane/retag.sh")
            it.into('build/crane')
            it.expand([
                imageId: imageId,
                version: project.version
            ])
        }

        // Note: even though this is a "build" task, it's really a pull-if-absent + tag task.
        registerDockerImage(project, 'tagLocalBuild') { DockerBuildImage build ->
            def dockerFileTask = dockerfile.get()

            build.group = 'Docker Registry'
            build.description = "Creates 'deephaven/${project.projectDir.name}:local-build'."
            build.inputs.files dockerFileTask.outputs.files
            build.dockerFile.set dockerFileTask.outputs.files.singleFile
            build.images.add("deephaven/${project.projectDir.name}:local-build".toString())
        }
    }

    static String registryProject(String name) {
        ":docker-${name}"
    }

    static Task registryTask(Project project, String name) {
        project.project(":docker-${name}").tasks.findByName('tagLocalBuild')
    }

    static FileCollection registryFiles(Project project, String name) {
        registryTask(project, name).outputs.files
    }
}
