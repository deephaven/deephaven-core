package io.deephaven.project.util

import com.github.jengelman.gradle.plugins.shadow.ShadowJavaPlugin
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.vanniktech.maven.publish.MavenPublishBaseExtension
import groovy.transform.CompileStatic
import io.deephaven.tools.License
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.BasePluginExtension
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPom
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository
import org.gradle.api.tasks.TaskProvider
import org.gradle.plugins.signing.SigningExtension

@CompileStatic
class PublishingTools {
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

    static final String SHADOW_PUBLICATION_NAME = 'shadow'

    static boolean isSnapshot(Project project) {
        return ((String)project.version).endsWith('-SNAPSHOT')
    }

    static void setupPublishing(Project project) {
        setupPublishingImpl project, false
        setupSigning project
    }

    static void setupBomPublishing(Project project) {
        // Note: setting up bom as separate entrypoint than setupPublishing because we can't use
        // io.deephaven.java-license-conventions to provide License since a the bom ('java-platform') can't use 'java'
        // nor 'java-library' plugin
        setupPublishingImpl project, true
        setupSigning project
    }

    private static void setupPublishingImpl(Project project, boolean isBom) {
        MavenPublishBaseExtension mpb = project.extensions.getByType(MavenPublishBaseExtension)
        BasePluginExtension base = project.extensions.getByType(BasePluginExtension)
        mpb.publishToMavenCentral()
        mpb.signAllPublications()
        mpb.pom { pom ->
            setPomConstants pom
            if (!isBom) {
                // Bom sets the license at the top level
                setPomLicense project, pom
            }
        }
        project.afterEvaluate {
            mpb.coordinates(null, base.archivesName.get(), null)
            mpb.pom { pom ->
                pom.name.set base.archivesName.get()
                pom.description.set project.description
            }
        }
        def assertIsRelease = assertIsReleaseTask(project)
        // Note: this is the same way that com.vanniktech.maven.publish.MavenPublishBaseExtension hooks into the
        // lower-level publish tasks to establish its own dependencies on the maven-publish tasks.
        // It uses `repo.name = "mavenCentral"` and the pattern as documented
        // https://docs.gradle.org/current/userguide/publishing_maven.html#publishing_maven:tasks,
        // publish[PubName]PublicationTo[RepoName]Repository
        project.tasks.withType(PublishToMavenRepository).configureEach { publishTask ->
            if (publishTask.name.endsWith("ToMavenCentralRepository")) {
                publishTask.dependsOn assertIsRelease
            }
        }
    }

    private static void setPomLicense(Project project, MavenPom pom) {
        License theLicense = project.extensions.extraProperties.get('license') as License
        pom.licenses { licenses ->
            licenses.license { license ->
                license.name.set theLicense.name
                license.url.set theLicense.url
            }
        }
    }

    private static void setPomConstants(MavenPom pom) {
        pom.url.set PROJECT_URL
        pom.organization { org ->
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

    static void setupSigning(Project project) {
        SigningExtension signingExtension = project.extensions.getByType(SigningExtension)
        signingExtension.required = "true" == project.findProperty('signingRequired')
        String signingKey = project.findProperty('signingKey')
        String signingPassword = project.findProperty('signingPassword')
        if (signingKey != null && signingPassword != null) {
            // In CI, it's harder to pass a file; so if specified, we use the in-memory version.
            signingExtension.useInMemoryPgpKeys(signingKey, signingPassword)
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
                def expectedGithubRef = isSnapshot(p)
                        ? 'refs/heads/main'
                        : "refs/heads/release/v${p.version}".toString()
                if (actualGithubRef != expectedGithubRef) {
                    throw new IllegalStateException("Release error: env GITHUB_REF '${actualGithubRef}' does not match expected '${expectedGithubRef}'. Bad tag? Bump version?")
                }
            }
        }
    }

    static void setupShadowName(Project project, String name) {
        project.tasks.named(ShadowJavaPlugin.SHADOW_JAR_TASK_NAME, ShadowJar) {
            it.archiveBaseName.set(name)
        }
        project.extensions.getByType(PublishingExtension).publications.named(SHADOW_PUBLICATION_NAME, MavenPublication) {
            it.artifactId = name
        }
        project.extensions.getByType(BasePluginExtension).archivesName.set(name)
    }
}
