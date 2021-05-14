package io.deephaven.tools

import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginConvention
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.Sync
import org.gradle.api.tasks.TaskProvider
import org.gradle.jvm.tasks.Jar

@CompileStatic
class License {
    private static final String INPUT_LICENSE_NAME = 'LICENSE'
    private static final String INPUT_NOTICE_NAME = 'NOTICE'

    private static final String OUTPUT_LICENSE_NAME = 'LICENSE'
    private static final String OUTPUT_NOTICE_NAME = 'NOTICE'

    private static final String LICENSE_SOURCE_SET_NAME = 'license'

    static License deephavenCommunityLicense(Project project) {
        return new License(
                project,
                project.rootProject.file('licenses/DCL-license.md'),
                project.rootProject.file('licenses/DCL-notice-template.md'))
    }

    /**
     * Creates a {@link License}. By default, projects will inherit the {@link #deephavenCommunityLicense(Project)}
     * unless there is a {@code LICENSE} and {@code NOTICE} file under the project's root directory.
     *
     * @param project the project
     * @return the license files for the {@code project}
     */
    static License createFrom(Project project) {
        if (project.file(INPUT_LICENSE_NAME).exists()) {
            if (!project.file(INPUT_NOTICE_NAME).exists()) {
                throw new IllegalStateException("Project '${project.name}' provides ${INPUT_LICENSE_NAME}, but not ${INPUT_NOTICE_NAME}")
            }
            return new License(project, project.file(INPUT_LICENSE_NAME), project.file(INPUT_NOTICE_NAME))
        }
        if (project.file(INPUT_NOTICE_NAME).exists()) {
            throw new IllegalStateException("Project '${project.name}' provides ${INPUT_NOTICE_NAME}, but not ${INPUT_LICENSE_NAME}")
        }
        // Otherwise, use the default project license
        return deephavenCommunityLicense(project)
    }

    Project project
    File license
    File notice

    private License(Project project, File license, File notice) {
        this.project = project
        this.license = license
        this.notice = notice
    }

    /**
     * Registers {@code this} license with the {@code project}.
     *
     * <p>Currently, registration consists creating a "license" source set and adding it into the jar.
     *
     * @param project the project
     */
    void register() {
        def licenseSourceSetDir = "${project.buildDir}/license-source-set"

        def syncLicenseData = syncSourceSetLicense(licenseSourceSetDir)

        // If we need to add the license and notice to each directory, we can run a mass
        // ./gradlew copyLicenseDataToProject
        /*
        def copyLicenseDataToSrc = project.tasks.register('copyLicenseDataToProject', Copy) {
            it.dependsOn(syncLicenseData)
            it.from("${licenseSourceSetDir}/META-INF/")
            it.into(project.projectDir)
        }*/

        // Create a "license" source set from build/license-source-set
        JavaPluginConvention java = project.convention.plugins.get('java') as JavaPluginConvention
        SourceSet licenseSourceSet = java.sourceSets.create(LICENSE_SOURCE_SET_NAME)
        licenseSourceSet.resources.srcDir(licenseSourceSetDir)

        // Ensure that processLicenseResources depends on syncLicenseData
        def processLicenseResourcesTask = project.tasks.findByName(licenseSourceSet.processResourcesTaskName)
        processLicenseResourcesTask.dependsOn(syncLicenseData)

        // Add the "license" source set into the jar.
        // Implicitly ensures that the jar task depends on the processLicenseResources task.
        Jar jar = project.tasks.findByName('jar') as Jar
        jar.from(processLicenseResourcesTask.outputs)
    }

    TaskProvider<Sync> syncSourceSetLicense(String licenseSourceSetDir) {
        def copyrightYear = '2021'
        syncLicensesProvider(
                project,
                'syncSourceSetLicense',
                "${licenseSourceSetDir}/META-INF",
                license.name,
                notice.name,
                copyrightYear,
                project.name)
    }

    TaskProvider<Sync> syncDockerLicense() {
        def copyrightYear = '2021'
        syncLicensesProvider(
                project,
                'syncDockerLicense',
                "${project.buildDir}/syncDockerLicense",
                license.name,
                notice.name,
                copyrightYear,
                project.name)
    }

    private TaskProvider<Sync> syncLicensesProvider(Project project, String taskName, String destDir, String licenseFilename, String noticeFilename, String copyrightYear, String projectName) {
        project.tasks.register(taskName, Sync) {
            it.from(license)
            it.from(notice)

            it.into(destDir)

            it.rename(licenseFilename, OUTPUT_LICENSE_NAME)
            it.rename(noticeFilename, OUTPUT_NOTICE_NAME)

            it.expand(copyrightYear: copyrightYear, projectName: project.name)

            // Make sure we invalidate this task if the structure changes
            it.inputs.property('OUTPUT_LICENSE_NAME', OUTPUT_LICENSE_NAME)
            it.inputs.property('OUTPUT_NOTICE_NAME', OUTPUT_NOTICE_NAME)

            it.inputs.property('licenseFilename', licenseFilename)
            it.inputs.property('noticeFilename', noticeFilename)

            it.inputs.property('copyrightYear', copyrightYear)
            it.inputs.property('projectName', projectName)
        }
    }
}