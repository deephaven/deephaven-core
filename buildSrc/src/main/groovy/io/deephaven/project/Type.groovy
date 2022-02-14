package io.deephaven.project

import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.ProjectDependency
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.TaskProvider
import org.gradle.language.base.plugins.LifecycleBasePlugin

@CompileStatic
class Type {

    public static final String VERIFY_ALL_PROJECTS_REGISTERED_TASK_NAME = 'verifyAllProjectsRegistered'

    public static final String VERIFY_RUNTIME_CLASSPATH_IS_PUBLIC_TASK_NAME = 'verifyRuntimeClasspathIsPublic'

    static void registerRoot(Project project) {
        register(project, 'root', false)
        project.tasks
                .getByName('quick')
                .dependsOn verifyAllRegisteredTask(project)
    }

    static void register(Project project, String type, boolean isPublic) {
        def ext = project.extensions.extraProperties
        if (ext.has(Type.class.name)) {
            throw new IllegalStateException("Unable to set project type '${project.name}' as '${type}'" +
                    " - is already registered with the type '${ext.get(Type.class.name)}'")
        }
        ext.set(Type.class.name, type)
        ext.set("${Type.class.name}.isPublic", isPublic)
        if (isPublic) {
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
                        throw new IllegalStateException("Project '${project.name}' has a dependency on a non-public project '${it.name}'")
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
        if (!ext.has(Type.class.name)) {
            throw new IllegalStateException("Public project '${project.name}' must declare a type. Please apply the appropriate 'io.deephaven.project.<type>' plugin.")
        }
    }

    static String getType(Project project) {
        def ext = project.extensions.extraProperties
        return ext.get(Type.class.name)
    }

    static boolean isPublic(Project project) {
        def ext = project.extensions.extraProperties
        return ext.get("${Type.class.name}.isPublic")
    }
}

