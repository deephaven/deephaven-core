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

import java.util.function.Consumer

@CompileStatic
class JavaDependencies {
    public static final String VERIFY_RUNTIME_CLASSPATH_IS_PUBLIC_TASK_NAME = 'verifyRuntimeClasspathIsPublic'

    public static final String VERIFY_RUNTIME_CLASSPATH_HAS_NO_PUBLIC_TESTING_DEPENDENCIES_TASK_NAME = 'verifyRuntimeClasspathHasNoPublicTestingDependencies'

    public static final String VERIFY_ALL_CONFIGURATIONS_ARE_PUBLIC_TASK_NAME = 'verifyAllConfigurationsArePublic'

    static TaskProvider<Task> verifyRuntimeClasspathIsPublicTask(Project project) {
        return project.tasks.register(VERIFY_RUNTIME_CLASSPATH_IS_PUBLIC_TASK_NAME) { task ->
            task.doLast {
                verifyRuntimeClasspathIsPublic(project)
            }
        }
    }

    static TaskProvider<Task> verifyRuntimeClasspathHasNoPublicTestingDependenciesTask(Project project) {
        return project.tasks.register(VERIFY_RUNTIME_CLASSPATH_HAS_NO_PUBLIC_TESTING_DEPENDENCIES_TASK_NAME) { task ->
            task.doLast {
                verifyRuntimeClasspathHasNoPublicTestingDependencies(project)
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

    private static void verifyRuntimeClasspathHasNoPublicTestingDependencies(Project project) {
        def runtimeClasspath = project.configurations.getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME)
        verifyConfigurationHasNoPublicTestingDependencies(project, runtimeClasspath)
    }

    private static void verifyAllConfigurationsArePublic(Project project) {
        for (Configuration configuration : project.configurations) {
            verifyConfigurationHasPublicDependencies(project, configuration)
        }
    }

    private static void allDependencies(Configuration configuration, Consumer<ProjectDependency> consumer) {
        configuration
                .getAllDependencies()
                .findAll { it instanceof ProjectDependency }
                .collect { it ->
                    (ProjectDependency)it
                }
                .each { it ->
                    consumer.accept(it)
                }
    }

    private static void allDependencyConstraints(Configuration configuration, Consumer<ProjectDependency> consumer) {
        configuration
                .getAllDependencyConstraints()
                .findAll { it instanceof DefaultProjectDependencyConstraint }
                .collect { it -> ((DefaultProjectDependencyConstraint) it).projectDependency }
                .each { it ->
                    consumer.accept(it)
                }
    }

    private static void verifyDefaultConfiguration(Project project, ProjectDependency dep) {
        if (dep.targetConfiguration != null && dep.targetConfiguration != 'default') {
            throw new IllegalStateException("Project '${project.name}' [${ProjectType.getType(project)}] has a dependency on the '${dep.targetConfiguration}' configuration of project '${dep.name}'")
        }
    }

    private static void verifyConfigurationHasPublicDependencies(Project project, Configuration configuration) {
        allDependencies(configuration, { dependency ->
            verifyDefaultConfiguration(project, dependency)
            def dp = dependency.dependencyProject
            if (!ProjectType.isPublic(dp)) {
                throw new IllegalStateException("Project '${project.name}' [${ProjectType.getType(project)}] has a dependency on a non-public project '${dp.name}' [${ProjectType.getType(dp)}]")
            }
        })
        allDependencyConstraints(configuration, { dependency ->
            verifyDefaultConfiguration(project, dependency)
            def dp = dependency.dependencyProject
            if (!ProjectType.isPublic(dp)) {
                throw new IllegalStateException("Project '${project.name}' [${ProjectType.getType(project)}] has a dependency constraint on a non-public project '${dp.name}' [${ProjectType.getType(dp)}]")
            }
        })
    }

    private static void verifyConfigurationHasNoPublicTestingDependencies(Project project, Configuration configuration) {
        allDependencies(configuration, { dependency ->
            verifyDefaultConfiguration(project, dependency)
            def dp = dependency.dependencyProject
            if (ProjectType.getType(dp) == ProjectType.JAVA_PUBLIC_TESTING) {
                throw new IllegalStateException("Project '${project.name}' [${ProjectType.getType(project)}] has a dependency on a testing project '${dp.name}' [${ProjectType.getType(dp)}]")
            }
        })
        allDependencyConstraints(configuration, { dependency ->
            verifyDefaultConfiguration(project, dependency)
            def dp = dependency.dependencyProject
            if (ProjectType.getType(dp) == ProjectType.JAVA_PUBLIC_TESTING) {
                throw new IllegalStateException("Project '${project.name}' [${ProjectType.getType(project)}] has a dependency constraint on a testing project '${dp.name}' [${ProjectType.getType(dp)}]")
            }
        })
    }
}
