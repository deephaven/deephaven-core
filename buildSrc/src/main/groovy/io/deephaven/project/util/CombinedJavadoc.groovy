package io.deephaven.project.util

import groovy.transform.CompileStatic
import io.deephaven.project.ProjectType
import org.gradle.api.Project

@CompileStatic
class CombinedJavadoc {

    static boolean includeProject(Project p) {
        def exclusions = [ 'hotspot-impl' ].toSet()
        ProjectType type = ProjectType.getType(p)
        if (!type.isPublic) {
            return false
        }
        if (exclusions.contains(p.name)) {
            return false
        }
        switch (type) {
            case ProjectType.BOM_PUBLIC:
                return false
            case ProjectType.JAVA_EXTERNAL:
            case ProjectType.JAVA_PUBLIC:
                return true
            default:
                throw new IllegalStateException("Unsure if public project type '${type}' is supposed to be included in combined-javadoc.")
        }
    }
}
