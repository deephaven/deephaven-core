import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.plugins.ExtraPropertiesExtension

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * The version the Python wheel builds stamp on their wheels.
 *
 * <p>Every wheel build must get the same value. Some wheels pin another wheel to their own version: deephaven-server
 * requires deephaven-core of the same version, and pydeephaven-ticking requires pydeephaven of the same version. If two
 * builds disagree, pip cannot install the wheels together.
 */
@CompileStatic
final class PythonVersion {
    /**
     * The Gradle property that turns on nightly versions, as in {@code ./gradlew -PnightlyWheels ...}.
     */
    static final String NIGHTLY_PROPERTY = 'nightlyWheels'

    private static final String CACHE_KEY = 'io.deephaven.pythonNightlyVersion'
    private static final DateTimeFormatter COMMIT_TIME =
            DateTimeFormatter.ofPattern('yyyyMMddHHmmss').withZone(ZoneOffset.UTC)
    private static final int HASH_LENGTH = 12

    /**
     * Returns the value the wheel builds get as {@code DEEPHAVEN_VERSION}. Each setup.py turns it into the wheel's
     * version with its {@code _normalize_version}.
     *
     * <p>Without {@code -PnightlyWheels} this is {@code project.version}, such as {@code 44.0-SNAPSHOT}, which setup.py
     * turns into {@code 44.0+snapshot}.
     *
     * <p>With {@code -PnightlyWheels} this is a PEP 440 dev release made from the checked-out commit, such as
     * {@code 44.0.dev20261124053012+g1a2b3c4d5e6f}. setup.py leaves it as it is. The parts are:
     * <ul>
     * <li>the release part of {@code project.version}, {@code 44.0};</li>
     * <li>{@code .dev} and the commit's committer time in UTC, to the second. It comes from the commit, not the build
     * clock, so every build of a commit gets the same version, and anyone can compute it from the commit with
     * {@code git show -s --format=%ct <sha>}. It grows with each merge to main, so {@code pip install --pre} picks the
     * newest nightly;</li>
     * <li>{@code +g} and the first 12 hex digits of the commit hash. pip ignores this part when a pin leaves it out,
     * so {@code ==44.0.dev20261124053012} still matches.</li>
     * </ul>
     * Deephaven Enterprise's nightly CI finds Core's nightly wheels by this scheme. Changing it breaks that build.
     */
    static String deephavenVersion(Project project) {
        return isNightly(project) ? nightlyVersion(project) : project.version.toString()
    }

    /**
     * Returns the version the wheels end up with, in Python's form. It repeats the rule in {@code _normalize_version}
     * in each setup.py: {@code 44.0-SNAPSHOT} becomes {@code 44.0+snapshot}. Keep the two in step. If they differ, an
     * install of a local wheel by this version fails to find the file.
     */
    static String wheelVersion(Project project) {
        if (isNightly(project)) {
            return nightlyVersion(project)
        }
        String[] parts = project.version.toString().split('-', 2)
        return parts.length == 2 ? "${parts[0]}+${parts[1].toLowerCase().replaceAll('[-_]', '.')}".toString() : parts[0]
    }

    static boolean isNightly(Project project) {
        return project.hasProperty(NIGHTLY_PROPERTY)
    }

    private static String nightlyVersion(Project project) {
        // Computed once per build and shared through the root project, so git runs once.
        ExtraPropertiesExtension extra = project.rootProject.extensions.extraProperties
        if (!extra.has(CACHE_KEY)) {
            String out = project.providers.exec { spec ->
                spec.workingDir(project.rootDir)
                spec.commandLine('git', 'show', '-s', '--format=%ct %H', 'HEAD')
            }.standardOutput.asText.get().trim()
            String[] fields = out.split(' ')
            String time = COMMIT_TIME.format(Instant.ofEpochSecond(Long.parseLong(fields[0])))
            String release = project.version.toString().split('-', 2)[0]
            extra.set(CACHE_KEY, "${release}.dev${time}+g${fields[1].substring(0, HASH_LENGTH)}".toString())
        }
        return extra.get(CACHE_KEY).toString()
    }
}
