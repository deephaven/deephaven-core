package io.deephaven.project.util

import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.gradle.api.artifacts.repositories.PasswordCredentials
import org.gradle.api.plugins.BasePluginConvention
import org.gradle.api.publish.Publication
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.tasks.TaskProvider
import org.gradle.plugins.signing.SigningExtension

@CompileStatic
class PublishingConstants {
    static final String DEVELOPER_ID = 'deephaven'
    static final String DEVELOPER_NAME = 'Deephaven Developers'
    static final String DEVELOPER_EMAIL = 'developers@deephaven.io'

    static final String PROJECT_URL = 'https://github.com/deephaven/deephaven-core'
    static final String ORG_NAME = 'Deephaven Data Labs'
    static final String ORG_URL = 'https://deephaven.io/'

    static final String ISSUES_SYSTEM = 'GitHub Issues'
    static final String ISSUES_URL = 'https://github.com/deephaven/deephaven-core/issues'

    static final String SCM_URL = 'https://github.com/deephaven/deephaven-core'
    static final String SCM_CONNECTION = 'scm:git:git://github.com/deephaven/deephaven-core.git'
    static final String SCM_DEV_CONNECTION = 'scm:git:ssh://github.com/deephaven/deephaven-core.git'

    static final String REPO_NAME = 'ossrh'
    static final String SNAPSHOT_REPO = 'https://s01.oss.sonatype.org/content/repositories/snapshots/'
    static final String RELEASE_REPO = 'https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/'

    static void setupRepositories(Project project) {
        PublishingExtension publishingExtension = project.extensions.getByType(PublishingExtension)
        publishingExtension.repositories { repoHandler ->
            repoHandler.maven { MavenArtifactRepository repo ->
                repo.name = REPO_NAME
                repo.url = ((String)project.version).endsWith('SNAPSHOT') ? SNAPSHOT_REPO : RELEASE_REPO
                // ossrhUsername, ossrhPassword
                repo.credentials(PasswordCredentials)
            }
        }
    }

    static void setupMavenPublication(Project project, MavenPublication mavenPublication) {
        mavenPublication.pom {pom ->
            pom.url.set PROJECT_URL
            pom.organization {org ->
                org.name.set ORG_NAME
                org.url.set ORG_URL
            }
            pom.scm { scm ->
                scm.url.set SCM_URL
                scm.connection.set SCM_CONNECTION
                scm.developerConnection.set SCM_DEV_CONNECTION
            }
            pom.issueManagement { im ->
                im.system.set ISSUES_SYSTEM
                im.url.set ISSUES_URL
            }
            pom.developers { devs ->
                devs.developer { dev ->
                    dev.id.set DEVELOPER_ID
                    dev.name.set DEVELOPER_NAME
                    dev.email.set DEVELOPER_EMAIL
                    dev.organization.set ORG_NAME
                    dev.organizationUrl.set ORG_URL
                }
            }
        }

        def publishToOssrhTask = project.tasks.getByName("publish${mavenPublication.getName().capitalize()}PublicationToOssrhRepository")

        publishToOssrhTask.dependsOn assertIsReleaseTask(project)

        project.afterEvaluate { Project p ->
            // https://central.sonatype.org/publish/requirements/
            if (p.description == null) {
                throw new IllegalStateException("Project '${project.name}' is missing a description, which is required for publishing to maven central")
            }
            BasePluginConvention base = p.convention.getPlugin(BasePluginConvention)
            // The common-conventions plugin should take care of this, but we'll double-check here
            if (!base.archivesBaseName.contains('deephaven')) {
                throw new IllegalStateException("Project '${project.name}' archiveBaseName '${base.archivesBaseName}' does not contain 'deephaven'")
            }
            mavenPublication.artifactId = base.archivesBaseName
            mavenPublication.pom { pom ->
                pom.name.set base.archivesBaseName
                pom.description.set p.description
            }
        }
    }

    static void setupSigning(Project project, Publication publication) {
        SigningExtension publishingExtension = project.extensions.getByType(SigningExtension)
        publishingExtension.sign(publication)
        String signingKey = project.findProperty('signingKey')
        String signingPassword = project.findProperty('signingPassword')
        if (signingKey != null && signingPassword != null) {
            // In CI, it's harder to pass a file; so if specified, we use the in-memory version.
            publishingExtension.useInMemoryPgpKeys(signingKey, signingPassword)
        }
    }

    static TaskProvider<Task> assertIsReleaseTask(Project p) {
        // todo: can we register this task once globally instead?
        return p.tasks.register('assertIsRelease') {task ->
            task.doLast {
                if (System.getenv('CI') != 'true') {
                    throw new IllegalStateException('Release error: env CI must be true')
                }
                def actualGithubRef = System.getenv('GITHUB_REF')
                def expectedGithubRef = "refs/heads/release/v${p.version}"
                if (actualGithubRef != expectedGithubRef) {
                    throw new IllegalStateException("Release error: env GITHUB_REF '${actualGithubRef}' does not match expected '${expectedGithubRef}'. Bad tag? Bump version?")
                }
            }
        }
    }
}
