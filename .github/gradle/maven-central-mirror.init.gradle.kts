// Gradle init script: resolve everything a build would fetch from Maven Central through a mirror,
// without changing the build.
//
// CI installs this into $GRADLE_USER_HOME/init.d (see .github/actions/gradle-setup) when MAVEN_CENTRAL_MIRROR_URL is set.
// The script fails the build if the variable is unset or blank: it is only meant to be applied deliberately, so
// a misconfigured mirror is an error, never a silent fallback to Maven Central.
//
// For one build:
//     MAVEN_CENTRAL_MIRROR_URL=https://maven-proxy.example.com ./gradlew build --init-script .github/gradle/maven-central-mirror.init.gradle.kts
//
// Every Maven repository whose URL is Maven Central is pointed at the mirror, wherever the build
// declares it: mavenCentral() or an explicit maven { url = ... }, in project repositories,
// buildscript {} blocks, settings' dependencyResolutionManagement, and pluginManagement. Name,
// content filters and position in the repository list are kept. No other repository is changed;
// in particular the Gradle Plugin Portal is still used directly. (The Portal itself redirects some
// downloads to Maven Central, and those don't go through the mirror.)
//
// If MAVEN_CENTRAL_MIRROR_USERNAME and MAVEN_CENTRAL_MIRROR_PASSWORD are set, they are sent to the
// mirror with HTTP Basic authentication on every request (preemptively, not after a 401).
// Credentials are only sent over HTTPS, or plain HTTP to this machine (for a local test mirror).
//
// Run with --info to see each repository that is redirected.
//
// Based on https://blog.gradle.org/maven-central-mirror, which matches repositories by the name
// mavenCentral() gives them; this matches by URL instead. Requires Gradle 6.8 or later.
// Gradle 9.8 added a built-in alternative that reads <mirror> entries from Maven's settings.xml
// (org.gradle.mirror.maven.settings=true); see
// https://docs.gradle.org/current/userguide/centralizing_repositories.html

import java.net.URI
import org.gradle.authentication.http.BasicAuthentication

apply<MavenCentralMirrorPlugin>()

class MavenCentralMirrorPlugin : Plugin<Gradle> {
    companion object {
        // Host and path of each URL that serves Maven Central. mavenCentral() uses the first.
        val MAVEN_CENTRAL = setOf("repo.maven.apache.org/maven2", "repo1.maven.org/maven2")
    }

    override fun apply(gradle: Gradle) {
        val configured = System.getenv("MAVEN_CENTRAL_MIRROR_URL")?.takeIf { it.isNotBlank() }
        if (configured == null) {
            throw GradleException("maven-central-mirror init script: MAVEN_CENTRAL_MIRROR_URL must be set")
        }
        val mirror = URI(configured.trimEnd('/') + "/")
        val logger = Logging.getLogger(MavenCentralMirrorPlugin::class.java)

        val mirrorUsername = System.getenv("MAVEN_CENTRAL_MIRROR_USERNAME")?.takeIf { it.isNotEmpty() }
        val mirrorPassword = System.getenv("MAVEN_CENTRAL_MIRROR_PASSWORD")?.takeIf { it.isNotEmpty() }
        if ((mirrorUsername == null) != (mirrorPassword == null)) {
            throw GradleException("maven-central-mirror init script: set both MAVEN_CENTRAL_MIRROR_USERNAME and MAVEN_CENTRAL_MIRROR_PASSWORD, or neither")
        }
        val loopback = setOf("localhost", "127.0.0.1", "[::1]", "::1")
        if (mirrorUsername != null && !mirror.scheme.equals("https", ignoreCase = true) && mirror.host !in loopback) {
            throw GradleException("maven-central-mirror init script: refusing to send credentials to $mirror over plain HTTP")
        }

        // Written for the Kotlin 1.4 API that Gradle 7 compiles init scripts against: no lowercase().
        fun isMavenCentral(url: URI?): Boolean {
            if (url == null) {
                return false
            }
            val scheme = url.scheme ?: return false
            val host = url.host ?: return false
            if (!scheme.equals("https", ignoreCase = true) && !scheme.equals("http", ignoreCase = true)) {
                return false
            }
            val hostAndPath = host + (url.path ?: "").trimEnd('/')
            return MAVEN_CENTRAL.any { it.equals(hostAndPath, ignoreCase = true) }
        }

        val useMirror = Action<MavenArtifactRepository> {
            if (isMavenCentral(url)) {
                logger.info("maven-central-mirror: repository '{}' {} -> {}", name, url, mirror)
                setUrl(mirror)
                if (mirror.scheme.equals("http", ignoreCase = true)) {
                    // Gradle refuses plain HTTP unless the repository opts in, e.g. for a local test mirror.
                    isAllowInsecureProtocol = true
                }
                if (mirrorUsername != null && mirrorPassword != null) {
                    credentials {
                        username = mirrorUsername
                        password = mirrorPassword
                    }
                    // Naming the scheme makes Gradle send credentials on the first request,
                    // instead of only after a 401 challenge.
                    authentication {
                        create<BasicAuthentication>("basic")
                    }
                }
            }
        }

        // configureEach applies to repositories already declared and to any declared later.
        val mirrorMavenCentral = Action<RepositoryHandler> {
            withType(MavenArtifactRepository::class.java).configureEach(useMirror)
        }

        gradle.beforeSettings(Action {
            mirrorMavenCentral.execute(buildscript.repositories)
            mirrorMavenCentral.execute(pluginManagement.repositories)
        })
        gradle.settingsEvaluated(Action {
            mirrorMavenCentral.execute(dependencyResolutionManagement.repositories)
            logger.lifecycle("maven-central-mirror: resolving Maven Central through {}", mirror)
        })
        gradle.beforeProject(Action {
            mirrorMavenCentral.execute(buildscript.repositories)
            mirrorMavenCentral.execute(repositories)
        })
    }
}
