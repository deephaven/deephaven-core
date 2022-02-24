package io.deephaven.project

import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.TaskProvider

@CompileStatic
enum ProjectType {

    BASIC(false, 'io.deephaven.project.basic'),
    DOCKER_REGISTRY(false, 'io.deephaven.project.docker-registry'),
    JAVA_EXTERNAL(true, 'io.deephaven.project.java-external'),
    JAVA_LOCAL(false, 'io.deephaven.project.java-local'),
    JAVA_PUBLIC(true, 'io.deephaven.project.java-public'),
    ROOT(false, 'io.deephaven.project.root');

    public static final String VERIFY_ALL_PROJECTS_REGISTERED_TASK_NAME = 'verifyAllProjectsRegistered'

    public static final String VERIFY_RUNTIME_CLASSPATH_IS_PUBLIC_TASK_NAME = 'verifyRuntimeClasspathIsPublic'

    static void register(Project project) {
        ProjectType type = getType(project)
        if (type == ROOT && project.rootProject != project) {
            throw new IllegalStateException("Project '${project.name}' is likely inheriting the 'ROOT' type - please set the property 'io.deephaven.project.ProjectType' as appropriate.")
        }

        project.pluginManager.apply(type.pluginId)
        registerInternal(project, type)
        if (type == ROOT) {
            project.tasks
                    .getByName('quick')
                    .dependsOn verifyAllRegisteredTask(project)
        }
    }

    private static void registerInternal(Project project, ProjectType projectType) {
        def key = "${ProjectType.class.name}.isRegistered"
        def ext = project.extensions.extraProperties
        if (ext.has(key)) {
            throw new IllegalStateException("Unable to set project type '${project.name}' as '${projectType}'" +
                    " - is already registered with the type '${ext.get(key)}'")
        }
        ext.set(key, projectType)
        if (projectType.isPublic) {
            project.tasks
                    .getByName('quick')
                    .dependsOn verifyRuntimeClasspathIsPublicTask(project)
        }
    }

    private static TaskProvider<Task> verifyRuntimeClasspathIsPublicTask(Project project) {
        return project.tasks.register(VERIFY_RUNTIME_CLASSPATH_IS_PUBLIC_TASK_NAME) { task ->
            task.doLast {
                verifyRuntimeClasspathIsPublic(project)
            }
        }
    }

    private static void verifyRuntimeClasspathIsPublic(Project project) {
        project
                .configurations
                .getByName(JavaPlugin.RUNTIME_CLASSPATH_CONFIGURATION_NAME)
                .getAllDependencies()
                .findAll { it instanceof ProjectDependency }
                .collect { ((ProjectDependency)it).dependencyProject }
                .each {
                    if (!isPublic(it)) {
                        throw new IllegalStateException("Public project '${project.name}' has a dependency on a non-public project '${it.name}'")
                    }
                }
    }

    private static TaskProvider<Task> verifyAllRegisteredTask(Project project) {
        return project.tasks.register(VERIFY_ALL_PROJECTS_REGISTERED_TASK_NAME) { task ->
            task.doLast {
                project.allprojects { Project p ->
                    verifyRegistered(p)
                }
            }
        }
    }

    private static void verifyRegistered(Project project) {
        def ext = project.extensions.extraProperties
        if (!ext.has("${ProjectType.class.name}.isRegistered")) {
            throw new IllegalStateException("Project '${project.name}' has not registered. Please apply the plugin 'io.deephaven.project.register'.")
        }
    }

    static ProjectType getType(Project project) {
        def typeString = project.findProperty('io.deephaven.project.ProjectType') as String
        if (typeString == null) {
            throw new IllegalStateException("Project '${project.name}' must declare a type. Please set the property 'io.deephaven.project.ProjectType'.")
        }
        return valueOf(typeString)
    }

    static boolean isPublic(Project project) {
        return getType(project).isPublic
    }

    boolean isPublic
    String pluginId

    ProjectType(boolean isPublic, String pluginId) {
        this.isPublic = isPublic
        this.pluginId = pluginId
    }
}

