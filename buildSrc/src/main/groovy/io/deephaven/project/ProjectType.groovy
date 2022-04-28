package io.deephaven.project

import groovy.transform.CompileStatic
import io.deephaven.project.util.JavaDependencies
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlatformPlugin
import org.gradle.api.plugins.JavaPlugin

@CompileStatic
enum ProjectType {

    BASIC(false, 'io.deephaven.project.basic'),
    DOCKER_REGISTRY(false, 'io.deephaven.project.docker-registry'),
    JAVA_EXTERNAL(true, 'io.deephaven.project.java-external'),
    JAVA_LOCAL(false, 'io.deephaven.project.java-local'),
    JAVA_PUBLIC(true, 'io.deephaven.project.java-public'),
    BOM_PUBLIC(true, 'io.deephaven.project.bom-public'),
    ROOT(false, 'io.deephaven.project.root');

    static void register(Project project) {
        ProjectType type = getType(project)
        if (type == ROOT && project.rootProject != project) {
            throw new IllegalStateException("Project '${project.name}' is likely inheriting the 'ROOT' type - please set the property 'io.deephaven.project.ProjectType' in file:${project.projectDir}/gradle.properties.")
        }
        registerInternal(project, type)
        project.pluginManager.apply(type.pluginId)
    }

    private static void registerInternal(Project project, ProjectType projectType) {
        def key = "${ProjectType.class.name}.isRegistered"
        def ext = project.extensions.extraProperties
        if (ext.has(key)) {
            throw new IllegalStateException("Unable to set project type '${project.name}' as '${projectType}'" +
                    " - is already registered with the type '${ext.get(key)}'")
        }
        ext.set(key, projectType)
    }


    static ProjectType getType(Project project) {
        def typeString = project.findProperty('io.deephaven.project.ProjectType') as String
        if (typeString == null) {
            throw new IllegalStateException("Project '${project.name}' must declare a type. Please set the property 'io.deephaven.project.ProjectType'.")
        }
        return valueOf(typeString)
    }

    static boolean isRegistered(Project project) {
        def ext = project.extensions.extraProperties
        return ext.has("${ProjectType.class.name}.isRegistered")
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

