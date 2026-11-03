package io.deephaven.tools

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.github.jengelman.gradle.plugins.shadow.transformers.MergeLicenseResourceTransformer
import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.plugins.BasePluginExtension
import org.gradle.api.plugins.JavaPluginExtension
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

    private static final String COPYRIGHT_YEARS = '2016-2026'

    // The jar tasks whose output is published. A shadow project publishes 'shadowJar' instead of 'jar', which receives
    // the license through transformers instead, see configureShadowJar.
    private static final List<String> PUBLISHED_JAR_TASK_NAMES = ['jar', 'sourcesJar', 'javadocJar']

    // The license and notice files that a shadowed dependency may contain; the first six are the defaults of
    // MergeLicenseResourceTransformer.
    private static final List<String> DEPENDENCY_LICENSE_PATHS = [
            'META-INF/LICENSE', 'META-INF/LICENSE.txt', 'META-INF/LICENSE.md', 'LICENSE', 'LICENSE.txt', 'LICENSE.md']
    private static final List<String> DEPENDENCY_NOTICE_PATHS = [
            'META-INF/NOTICE', 'META-INF/NOTICE.txt', 'META-INF/NOTICE.md', 'NOTICE', 'NOTICE.txt', 'NOTICE.md']

    private static final String APACHE_LICENSE_SHA256 = 'cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30'
    static final String APACHE_LICENSE_NAME = 'The Apache License, Version 2.0'
    static final String APACHE_LICENSE_URL = 'https://www.apache.org/licenses/LICENSE-2.0.txt'

    static License deephavenCommunityLicense(Project project) {
        return new License(
                project,
                'Deephaven Community License Agreement 1.0',
                'https://github.com/deephaven/deephaven-core/blob/main/LICENSE.md',
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
        File inputLicenseFile = project.file(INPUT_LICENSE_NAME)
        if (inputLicenseFile.exists()) {
            if (!project.file(INPUT_NOTICE_NAME).exists()) {
                throw new IllegalStateException("Project '${project.name}' provides ${INPUT_LICENSE_NAME}, but not ${INPUT_NOTICE_NAME}")
            }
            def inputLicenseFileSha256 = inputLicenseFile.text.digest("SHA-256")
            switch (inputLicenseFileSha256) {
                case APACHE_LICENSE_SHA256:
                    return new License(project, APACHE_LICENSE_NAME, APACHE_LICENSE_URL, inputLicenseFile, project.file(INPUT_NOTICE_NAME))
            }
            throw new IllegalStateException("Found new license in project '${project.name}'. We must add support in 'io.deephaven.tools.License'. sha-256:${inputLicenseFileSha256}")
        }
        if (project.file(INPUT_NOTICE_NAME).exists()) {
            throw new IllegalStateException("Project '${project.name}' provides ${INPUT_NOTICE_NAME}, but not ${INPUT_LICENSE_NAME}")
        }
        // Otherwise, use the default project license
        return deephavenCommunityLicense(project)
    }

    Project project
    String name
    String url
    File license
    File notice

    private License(Project project, String name, String url, File license, File notice) {
        this.project = project
        this.name = name
        this.url = url
        this.license = license
        this.notice = notice
    }

    /**
     * Registers {@code this} license with the {@code project}.
     *
     * <p>Currently, registration consists creating a "license" source set and adding it into the jar, sources jar and
     * javadoc jar. When the project is shadowed, the shadow jar instead gets a license and notice that start with
     * {@code this} license and notice and append those of the shadowed dependencies.
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
        JavaPluginExtension java = project.extensions.findByType(JavaPluginExtension)

        SourceSet licenseSourceSet = java.sourceSets.create(LICENSE_SOURCE_SET_NAME)
        licenseSourceSet.resources.srcDir(licenseSourceSetDir)

        // Ensure that processLicenseResources depends on syncLicenseData
        def processLicenseResourcesTask = project.tasks.findByName(licenseSourceSet.processResourcesTaskName)
        processLicenseResourcesTask.dependsOn(syncLicenseData)

        // Add the "license" source set into the published jars.
        // Implicitly ensures that the jar tasks depend on the processLicenseResources task.
        project.tasks.withType(Jar).configureEach { Jar jar ->
            if (jar.name in PUBLISHED_JAR_TASK_NAMES) {
                jar.from(processLicenseResourcesTask.outputs)
            }
        }

        project.plugins.withId('com.gradleup.shadow') {
            project.tasks.named(ShadowJar.SHADOW_JAR_TASK_NAME, ShadowJar).configure { ShadowJar shadowJar ->
                configureShadowJar(shadowJar, syncLicenseData, "${licenseSourceSetDir}/META-INF")
            }
        }
    }

    private void configureShadowJar(ShadowJar shadowJar, TaskProvider<Sync> syncLicenseData, String licenseDir) {
        shadowJar.dependsOn(syncLicenseData)

        // The transformers must see every dependency's copy, not just the first one.
        shadowJar.filesMatching(DEPENDENCY_LICENSE_PATHS + DEPENDENCY_NOTICE_PATHS) { details ->
            details.duplicatesStrategy = DuplicatesStrategy.INCLUDE
        }

        shadowJar.transform(MergeLicenseResourceTransformer) { MergeLicenseResourceTransformer transformer ->
            transformer.setIncludes(DEPENDENCY_LICENSE_PATHS)
            transformer.artifactLicense.set(project.file("${licenseDir}/${OUTPUT_LICENSE_NAME}"))
            transformer.artifactLicenseSpdxId.set(name == APACHE_LICENSE_NAME ? 'Apache-2.0' : '')
        }

        shadowJar.transform(MergeLicenseResourceTransformer) { MergeLicenseResourceTransformer transformer ->
            transformer.setIncludes(DEPENDENCY_NOTICE_PATHS)
            transformer.outputPath.set("META-INF/${OUTPUT_NOTICE_NAME}".toString())
            transformer.artifactLicense.set(project.file("${licenseDir}/${OUTPUT_NOTICE_NAME}"))
            transformer.artifactLicenseSpdxId.set('')
            transformer.firstSeparator.set("""
${'-' * 120}

This artifact includes dependencies with the following notices:
---------------------------------------------------------------
""".toString())
        }
    }

    TaskProvider<Sync> syncSourceSetLicense(String licenseSourceSetDir) {
        def archivesBaseName = (project.extensions.findByType(BasePluginExtension)).archivesName.get()
        syncLicensesProvider(
                project,
                'syncSourceSetLicense',
                "${licenseSourceSetDir}/META-INF",
                license.name,
                notice.name,
                COPYRIGHT_YEARS,
                archivesBaseName)
    }

    TaskProvider<Sync> syncDockerLicense() {
        def archivesBaseName = project.extensions.findByType(BasePluginExtension).archivesName.get()
        syncLicensesProvider(
                project,
                'syncDockerLicense',
                "${project.buildDir}/syncDockerLicense",
                license.name,
                notice.name,
                COPYRIGHT_YEARS,
                archivesBaseName)
    }

    private TaskProvider<Sync> syncLicensesProvider(Project project, String taskName, String destDir, String licenseFilename, String noticeFilename, String copyrightYear, String archivesBaseName) {
        project.tasks.register(taskName, Sync) {
            it.from(license)
            it.from(notice)

            it.into(destDir)

            it.rename(licenseFilename, OUTPUT_LICENSE_NAME)
            it.rename(noticeFilename, OUTPUT_NOTICE_NAME)

            it.expand(copyrightYear: copyrightYear, projectName: archivesBaseName)

            // Make sure we invalidate this task if the structure changes
            it.inputs.property('OUTPUT_LICENSE_NAME', OUTPUT_LICENSE_NAME)
            it.inputs.property('OUTPUT_NOTICE_NAME', OUTPUT_NOTICE_NAME)

            it.inputs.property('licenseFilename', licenseFilename)
            it.inputs.property('noticeFilename', noticeFilename)

            it.inputs.property('copyrightYear', copyrightYear)
            it.inputs.property('projectName', archivesBaseName)
        }
    }
}