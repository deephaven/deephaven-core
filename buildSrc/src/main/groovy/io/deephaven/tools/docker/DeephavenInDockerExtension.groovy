package io.deephaven.tools.docker

import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerExecContainer
import com.bmuschko.gradle.docker.tasks.container.DockerInspectContainer
import com.bmuschko.gradle.docker.tasks.container.DockerLogsContainer
import com.bmuschko.gradle.docker.tasks.container.DockerRemoveContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.image.DockerBuildImage
import com.bmuschko.gradle.docker.tasks.network.DockerCreateNetwork
import com.bmuschko.gradle.docker.tasks.network.DockerRemoveNetwork
import com.github.dockerjava.api.command.InspectContainerResponse
import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property
import org.gradle.api.specs.Spec
import org.gradle.api.specs.Specs
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
    final TaskProvider<? extends Task> logTask
    final TaskProvider<? extends Task> threadDumpTask

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

    /**
     * A condition to test to see if server logs should be printed to the build output.
     */
    abstract Property<Spec<Task>> getShouldLog();

    @Inject
    DeephavenInDockerExtension(Project project) {
        // The server is healthy once jpy and the UpdateGraph have started, which takes on the order of 20 seconds on a
        // loaded CI runner; the container reports 'starting' until then, and this poller only accepts 'healthy'.
        awaitStatusTimeout.set 60
        checkInterval.set 100
        shouldLog.set Specs.satisfyNone()

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
            task.onNext { Object obj ->
                def inspect = (InspectContainerResponse) obj
                getPort().set(Integer.parseInt(inspect.getNetworkSettings().ports.bindings.values().first()[0].hostPortSpec))
            }
        }

        logTask = project.tasks.register('writeServerLogs', DockerLogsContainer) { task ->
            task.targetContainerId containerName.get()

            task.onlyIf = Specs.union(
                    shouldLog.get(),
                    Specs.convertClosureToSpec {
                        return startTask.get().state.failure != null
                    },
                    Specs.convertClosureToSpec {
                        return healthyTask.get().state.failure != null
                    },
            )
        }

        // Only runs when a task registered via shouldLogIfTaskFails has failed. The container is still up at that
        // point (stopDeephaven is ordered after this), so even though the client-side test has ended, a server-side
        // deadlock or stuck thread will still be visible. The server image is a full JDK and its entrypoint execs
        // java, so jcmd can attach to PID 1; the dump is written to this task's stdout.
        threadDumpTask = project.tasks.register('threadDumpServer', DockerExecContainer) { task ->
            task.targetContainerId containerName.get()
            task.commands.add(['jcmd', '1', 'Thread.print', '-l'] as String[])

            task.onlyIf = Specs.convertClosureToSpec { Task t ->
                shouldLog.get().isSatisfiedBy(t)
            }
        }
        logTask.configure { Task t ->
            // cosmetic: print the dump before the (much longer) server logs
            t.mustRunAfter(threadDumpTask)
        }

        endTask = project.tasks.register('stopDeephaven', DockerRemoveContainer) { task ->
            task.dependsOn createDeephavenGrpcApi
//            task.dependsOn logTask// this seems to prevent start/healthy task from triggering logs
            task.mustRunAfter(logTask, threadDumpTask)
            task.finalizedBy removeDeephavenGrpcApiNetwork

            task.targetContainerId containerName.get()
            task.force.set true
            task.removeVolumes.set true
        }
        // if start or health check fails, ensure that we write logs and stop before exiting
        startTask.configure {
            it.finalizedBy logTask, endTask
        }
        healthyTask.configure {
            it.finalizedBy logTask, endTask
        }
    }

    /**
     * If the given task fails, take a thread dump of the server and print server logs to console. This implies
     * that the thread dump and log tasks can't run until after the provided task has ended.
     */
    void shouldLogIfTaskFails(TaskProvider<? extends Task> task) {
        task.configure { Task t ->
            t.finalizedBy threadDumpTask, logTask
            shouldLog.set(Specs.convertClosureToSpec({
                t.state.failure != null
            }))
        }
    }
}
