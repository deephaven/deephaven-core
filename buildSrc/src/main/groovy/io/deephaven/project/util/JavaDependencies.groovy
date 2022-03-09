package io.deephaven.project.util

import groovy.transform.CompileStatic
import io.deephaven.project.ProjectType
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependencyConstraint
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.TaskProvider

@CompileStatic
class JavaDependencies {
    public static final String VERIFY_RUNTIME_CLASSPATH_IS_PUBLIC_TASK_NAME = 'verifyRuntimeClasspathIsPublic'

    public static final String VERIFY_ALL_CONFIGURATIONS_ARE_PUBLIC_TASK_NAME = 'verifyAllConfigurationsArePublic'

    static TaskProvider<Task> verifyRuntimeClasspathIsPublicTask(Project project) {
        return project.tasks.register(VERIFY_RUNTIME_CLASSPATH_IS_PUBLIC_TASK_NAME) { task ->
            task.doLast {
                verifyRuntimeClasspathIsPublic(project)
            }
        }
    }

    static TaskProvider<Task> verifyAllConfigurationsArePublicTask(Project project) {
        return project.tasks.register(VERIFY_ALL_CONFIGURATIONS_ARE_PUBLIC_TASK_NAME) { task ->
            task.doLast {
                verifyAllConfigurationsArePublic(project)
            }
        }
    }

    private static void verifyRuntimeClasspathIsPublic(Project project) {
        def runtimeClasspath = project.configurations.getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME)
        verifyConfigurationHasPublicDependencies(project, runtimeClasspath)
    }


    private static void verifyAllConfigurationsArePublic(Project project) {
        for (Configuration configuration : project.configurations) {
            verifyConfigurationHasPublicDependencies(project, configuration)
        }
    }

    private static void verifyConfigurationHasPublicDependencies(Project project, Configuration configuration) {
        configuration
                .getAllDependencies()
                .findAll { it instanceof ProjectDependency }
                .collect { ((ProjectDependency)it).dependencyProject }
                .each {
                    if (!ProjectType.isPublic(it)) {
                        throw new IllegalStateException("Project '${project.name}' [${ProjectType.getType(project)}] has a dependency on a non-public project '${it.name}' [${ProjectType.getType(it)}]")
                    }
                }
        configuration
                .getAllDependencyConstraints()
                .findAll { it instanceof DefaultProjectDependencyConstraint }
                .collect {((DefaultProjectDependencyConstraint)it).projectDependency.dependencyProject }
                .each {
                    if (!ProjectType.isPublic(it)) {
                        throw new IllegalStateException("Project '${project.name}' [${ProjectType.getType(project)}] has a dependency constraint on a non-public project '${it.name}' [${ProjectType.getType(it)}]")
                    }
                }
    }
}
