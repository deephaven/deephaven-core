package io.deephaven.tools

import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginConvention
import org.gradle.api.tasks.SourceSet

/**
 * Some useful helper methods for dealing with java projects.
 */
@CompileStatic
class Java {

    /**
     * Extract a SourceSet from a given project
     *
     * @param project - Where we will get the sourceSet from
     * @param name - The name of the sourceSet (default value is 'main')
     * @return - The named sourceSet.  If it doesn't exist, an exception is thrown.
     */
    static SourceSet sourceSet(Project project, String name = 'main') {
        JavaPluginConvention java = project.convention.plugins.get('java') as JavaPluginConvention
        return java.sourceSets.getByName(name)
    }
}
