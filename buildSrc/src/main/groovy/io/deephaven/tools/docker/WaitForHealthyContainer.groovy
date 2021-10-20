package io.deephaven.tools.docker

import com.bmuschko.gradle.docker.tasks.container.DockerExistingContainer
import com.github.dockerjava.api.command.InspectContainerCmd
import com.github.dockerjava.api.command.InspectContainerResponse
import groovy.transform.CompileStatic
import org.gradle.api.GradleException
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional

@CompileStatic
class WaitForHealthyContainer extends DockerExistingContainer {
    /**
     * Wait timeout in seconds.
     */
    @Input
    @Optional
    final Property<Integer> awaitStatusTimeout = project.objects.property(Integer)

    /**
     * Interval between each check in milliseconds.
     */
    @Input
    @Optional
    final Property<Integer> checkInterval = project.objects.property(Integer)

    @Override
    void runRemoteCommand() {
        logger.quiet "Waiting for container with ID '${containerId.get()}' to be healthy."

        InspectContainerCmd command = dockerClient.inspectContainerCmd(containerId.get())
        long deadline = System.currentTimeMillis() + awaitStatusTimeout.getOrElse(30) * 1000
        long sleepInterval = checkInterval.getOrElse(500)

        while(!check(deadline, command)) {
            sleep(sleepInterval)
        }
    }


    private boolean check(Long deadline, InspectContainerCmd command) {
        if (deadline && System.currentTimeMillis() > deadline) {
            throw new GradleException('Health check timeout expired')
        }

        InspectContainerResponse response = command.exec()
        InspectContainerResponse.ContainerState state = response.state
        if (!state.running) {
            throw new GradleException("Container with ID '${getContainerId()}' is not running")
        }

        String healthStatus
        if (state.health) {
            healthStatus = state.health.status
        } else {
            logger.quiet 'HEALTHCHECK instruction was not used to build this image. Falling back to generic Status of container...'
            healthStatus = state.status
        }

        if (nextHandler) {
            nextHandler.execute(healthStatus)
        }
        return healthStatus ==~ /(healthy|running)/
    }
}
