import com.bmuschko.gradle.docker.tasks.container.DockerCopyFileFromContainer
import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerLogsContainer
import com.bmuschko.gradle.docker.tasks.container.DockerRemoveContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerWaitContainer
import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import com.bmuschko.gradle.docker.tasks.image.DockerRemoveImage
import com.bmuschko.gradle.docker.tasks.image.Dockerfile
import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.Sync
import org.gradle.api.tasks.TaskProvider
import org.gradle.util.ConfigureUtil

@CompileStatic
class Docker {
    static class DockerTaskConfig {
        private Action<? super CopySpec> copyIn;
        private Action<? super Sync> copyOut;
        private File dockerfileFile;
        private Action<? super Dockerfile> dockerfileAction;

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
         * Resulting files to copy out from the containerOutPath
         */
        DockerTaskConfig copyOut(Action<? super Sync> action) {
            copyOut = action;
            return this;
        }
        /**
         * Resulting files to copy out from the containerOutPath
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
         * Path inside the created docker container that contains the output to be copied out as part of this task
         */
        String containerOutPath = '/out'
        /**
         * List of any containers, the tasks that create them
         */
        List<Task> parentContainers = []

        /**
         * Optional command to run whenever the task is invoked, otherwise the image's contents will be used
         * as-is.
         */
        List<String> command;
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
        cfg.imageName = "deephaven/${taskName}"

        // ask for more configuration
        action.execute(cfg);

        String dockerContainerName = "$taskName-container-${UUID.randomUUID()}"
        String dockerCopyLocation = "${project.buildDir}/$taskName-tmp-copy"
        // This directory is always marked as the inputs for the subsequent DockerBuildImage task, setting it here to
        // be explicit and scoped to this task
        File dockerWorkspaceContents = project.file("${project.buildDir}/$taskName-docker");

        def clean = project.tasks.findByName('clean');

        // If needed, make a Dockerfile from config
        TaskProvider<Dockerfile> dockerfileTask

        if (cfg.dockerfileAction && cfg.dockerfileFile) {
            throw new IllegalStateException("Cannot specify dockerfile as both path and closure")
        }

        if (cfg.dockerfileAction) {
            dockerfileTask = project.tasks.register("${taskName}Dockerfile", Dockerfile) { dockerfile ->
                cfg.dockerfileAction.execute(dockerfile)
            }
        }

        // Copy the requested files into build/docker
        def prepareDocker = project.tasks.register("${taskName}PrepareDocker", Sync) { sync ->
            // First, apply the provided spec
            cfg.copyIn.execute(sync)

            // Then, make sure we write into our docker dir
            sync.into dockerWorkspaceContents

            if (cfg.dockerfileFile) {
                sync.from cfg.dockerfileFile
            } else if (cfg.dockerfileAction) {
                sync.from dockerfileTask.get().outputs.files
            }
        }

        // Produce a docker image from the copied inputs and provided dockerfile, and tag it
        TaskProvider<DockerBuildImage> makeImage = project.tasks.register("${taskName}MakeImage", DockerBuildImage) { buildImage ->
            buildImage.with {
                // assign our own workspace dir
                inputDir.set dockerWorkspaceContents
                // set it as inputs anyway, in case this changes, as it is also an implicit dependsOn
                inputs.files prepareDocker.get().outputs.files

                // specify that we rely on any parent container's outputs
                inputs.files cfg.parentContainers.each { t -> t.outputs.files }

                // specify tag
                images.add(cfg.imageName)
            }
        }

        // Enabling removing the image as part of clean task
        TaskProvider<DockerRemoveImage> removeImage = project.tasks.register("${taskName}DeleteImage", DockerRemoveImage) { removeImage ->
            removeImage.with {
                //TODO wire this up to not even run if the image doesn't exist
                // we use imageName
                targetImageId cfg.imageName
                onError { t ->
                    // ignore, the image might not exist
                }
            }
        }
        clean.dependsOn removeImage

        // Create a new container from the image above, as a workaround to extract the output from the dockerfile's
        // build steps
        TaskProvider<DockerCreateContainer> createContainer = project.tasks.register("${taskName}CreateContainer", DockerCreateContainer) { createContainer ->
            createContainer.with {
                // this could probably be simplified to a dependsOn, since we already use its imageId as an input
                inputs.files makeImage.get().outputs.files

                if (cfg.command) {
                    // if provided, set a run command that we'll use each time it starts
                    entrypoint.set(cfg.command)

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
                onlyIf {
                    cfg.command && containerFinished.get().exitCode != 0
                }
            }
        }
        containerFinished.configure { waitCommand -> waitCommand.finalizedBy(containerLogs) }

        // Copy the results from the build out of the container, so the sync task can make it available
        TaskProvider<DockerCopyFileFromContainer> copyGenerated = project.tasks.register("${taskName}CopyGeneratedOutput", DockerCopyFileFromContainer) { copy ->
            copy.with {
                if (cfg.command) {
                    dependsOn containerFinished
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

                    if (cfg.command && containerFinished.get().exitCode != 0) {
                        throw new GradleException("Command '${cfg.command.join(' ')}' failed with exit code ${containerFinished.get().exitCode}, check logs for details")
                    }
                }
            }
        }

        // Sync the results to where the caller requested us to put them
        return project.tasks.register(taskName, Sync) { sync ->
            sync.with {
                dependsOn copyGenerated

                // run the provided closure first
                cfg.copyOut.execute(sync)

                // then set the from location
                from dockerCopyLocation
            }
        }
    }
}
