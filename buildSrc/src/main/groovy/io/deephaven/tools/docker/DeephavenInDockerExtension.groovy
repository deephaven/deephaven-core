package io.deephaven.tools.docker

import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerInspectContainer
import com.bmuschko.gradle.docker.tasks.container.DockerRemoveContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import com.bmuschko.gradle.docker.tasks.network.DockerCreateNetwork
import com.bmuschko.gradle.docker.tasks.network.DockerRemoveNetwork
import com.github.dockerjava.api.command.InspectContainerResponse
import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.TaskProvider

import javax.inject.Inject

/**
 * Extension to manage tasks around starting and stopping a Deephaven Core instance. Presently, to enable support for
 * python in the server, this uses docker.
 *
 * This isn't very configurable at this time, but the kinds of projects that will use this don't yet need a lot of
 * flexibility.
 */
@CompileStatic
public abstract class DeephavenInDockerExtension {
    final TaskProvider<? extends Task> startTask
    final TaskProvider<? extends Task> healthyTask
    final TaskProvider<DockerInspectContainer> portTask
    final TaskProvider<? extends Task> endTask

    final String deephavenServerProject
    final String serverTask

    abstract Property<String> getNetworkName()

    abstract Property<String> getContainerName()

    abstract Property<Integer> getAwaitStatusTimeout()
    abstract Property<Integer> getCheckInterval()

    abstract MapProperty<String, String> getEnvVars();

    /**
     * Makes the exposed port available to other docker tasks. Rather than hardcode a particular
     * port, docker will select one (allowing for multiple parallel running instances), and expose
     * it here after the "waitForPort" task is complete.
     */
    abstract Property<Integer> getPort();

    @Inject
    DeephavenInDockerExtension(Project project) {
        awaitStatusTimeout.set 20
        checkInterval.set 100

        // irritating configuration order of operations to work out here, so just leaving
        // these as constants until we decide they aren't any more
        deephavenServerProject = ':docker-server-jetty'
        serverTask = 'buildDocker-server-jetty'
        def serverProject = project.evaluationDependsOn(deephavenServerProject)

        def createDeephavenGrpcApiNetwork = project.tasks.register('createDeephavenGrpcApiNetwork', DockerCreateNetwork) { task ->
            task.networkName.set networkName.get()
        }
        def removeDeephavenGrpcApiNetwork = project.tasks.register('removeDeephavenGrpcApiNetwork', DockerRemoveNetwork) {task ->
            task.networkId.set networkName.get()
        }

        def createDeephavenGrpcApi = project.tasks.register('createDeephavenGrpcApi', DockerCreateContainer) { task ->
            DockerBuildImage grpcApiImage = serverProject.tasks.findByName(serverTask) as DockerBuildImage

            task.dependsOn(grpcApiImage, createDeephavenGrpcApiNetwork)
            task.targetImageId grpcApiImage.getImageId()
            task.containerName.set containerName.get()
            task.hostConfig.network.set networkName.get()
            task.envVars.set(this.getEnvVars())

            // In the jetty image, port 10000 is the http server
            task.exposePorts("tcp", [10000])
            task.hostConfig.portBindings.set(["10000"])
        }

        startTask = project.tasks.register('startDeephaven', DockerStartContainer) { task ->
            task.dependsOn createDeephavenGrpcApi
            task.containerId.set containerName.get()
        }

        healthyTask = project.tasks.register('waitForHealthy', WaitForHealthyContainer) { task ->
            task.dependsOn startTask

            task.awaitStatusTimeout.set this.awaitStatusTimeout.get()
            task.checkInterval.set this.checkInterval.get()

            task.containerId.set containerName.get()
        }

        portTask = project.tasks.register('waitForPort', DockerInspectContainer) {task ->
            task.dependsOn healthyTask
            task.containerId.set containerName.get()
            task.onNext { InspectContainerResponse inspect ->
                getPort().set(Integer.parseInt(((InspectContainerResponse) inspect).getNetworkSettings().ports.bindings.values().first()[0].hostPortSpec))
            }
        }

        endTask = project.tasks.register('stopDeephaven', DockerRemoveContainer) { task ->
            task.dependsOn createDeephavenGrpcApi
            task.finalizedBy removeDeephavenGrpcApiNetwork

            task.targetContainerId containerName.get()
            task.force.set true
            task.removeVolumes.set true
        }
    }
}
