import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.internal.os.OperatingSystem

import static PythonConstants.jpy_platform_name
import static PythonConstants.windows

/**
 *
 * We can be very explicit about which versions we want to test.
 *
 * It's likely that we should always be testing against the latest patch version for each minor
 * release that we want to support (ie, latest 2.7.x, 3.6.x, 3.7.x).
 *
 * We can do a fuller back-version testing in CI if desired.
 *
 * Note: it takes about 2 minutes to bootstrap each version - I've kept the bootstrap dir
 * outside of the normal build dir (see bootstrapDir). Once the bootstrap is downloaded and
 * compiled, we want to keep it around as long as possible.
 */
@CompileStatic
enum PythonVersion {
    PY_36('3', '6', '8', '64', windows ? 'python36' : 'python3.6m',
            windows ? 'jpy.pyd' : "jpy.cpython-36m-${jpy_platform_name}.so", windows ? 'jdl.pyd' : "jdl.cpython-36m-${jpy_platform_name}.so"),
    PY_37('3', '7', '10', '64', (windows ? 'python37' : 'python3.7m'),
            windows ? 'jpy.pyd' : "jpy.cpython-37m-${jpy_platform_name}.so", windows ? 'jdl.pyd' : "jdl.cpython-37m-${jpy_platform_name}.so"),
    ;

    static PythonVersion BUILD_DH = PY_37
    static PythonVersion BUILD_JUPYTER_GRID = PY_37

    String major
    String minor
    String patch
    String bits

    String basePyLib
    String jpyLib
    String jdlLib

    PythonVersion(String major, String minor, String patch, String bits, String basePyLib, String jpyLib, String jdlLib) {
        this.major = major
        this.minor = minor
        this.patch = patch
        this.bits = bits
        this.basePyLib = basePyLib
        this.jpyLib = jpyLib
        this.jdlLib = jdlLib
    }

    String getName() {
        return "python${bits}-${major}.${minor}.${patch}".toString()
    }

    String getVersion() {
        return "${major}.${minor}.${patch}".toString()
    }

    String getPyLib(bootstrapDir) {
        return "${bootstrapDir}/${name}/lib/${OperatingSystem.current().getSharedLibraryName(basePyLib.toString())}".toString()
    }

    String getJpyLib(File venvDir, VenvType type) {
        return "${venvDir}/${getVenvName(type)}/lib/python${major}.${minor}/site-packages/${jpyLib}".toString()
    }

    String getJdlLib(File venvDir, VenvType type) {
        return "${venvDir}/${getVenvName(type)}/lib/python${major}.${minor}/site-packages/${jdlLib}".toString()
    }

    String getWheelDir(File buildDir, String projectVersion, VenvType type, PythonVersion version) {
        // added projectVersion to the wheel dir, since we are installing by directory, we want to make
        // sure our build system always installs the correct one
        // (and the python version is *normalized*, so it's a bit tougher for us to get the actual whl
        // name)
        return "${buildDir}/$version.projectName/dist-${projectVersion}-${getVenvName(type)}".toString()
    }

    String getVenvName(VenvType type) {
        return "$type.venvName-$name".toString()
    }

    /**
     * This is the name of the ext property where we will attach our publishing artifact to the project.
     * This is used by Publishing.groovy to find the PublishArtifact needed to add artifacts to our publications.
     */
    String getPublicationName() {
        return "Py${major}.${minor}".toString()
    }

    String getProjectPathForDebug() {
        return "project('$projectPath')"
    }

    String getProjectPath() {
        return ":${projectName}"
    }

    String getProjectName() {
        return "py${major}${minor}"
    }

    String getRpmPath() {
        return "python/envs/${projectName}"
    }

    String getCreateVenvTaskName(VenvType type) {
        return "Create_virtualenv_'${getVenvName(type)}'".toString()
    }

    String getBootstrapTaskName() {
        return "Bootstrap_PYTHON_'${name}'".toString()
    }

    Map<String, String> getRunProperties(File bootstrapDir, File venvDir, VenvType type) {
        return [
                'jpy.pythonLib':getPyLib(bootstrapDir),
                'jpy.jpyLib':getJpyLib(venvDir, type),
                'jpy.jdlLib':getJdlLib(venvDir, type),
                'jpy.programName': "${venvDir}/$execPy".toString(),
        ]
    }

    String getExecPy() {
        return OperatingSystem.current().isWindows() ? "Scripts/python.exe" : "bin/python"
    }

    String getExecPip() {
        return OperatingSystem.current().isWindows() ? "Scripts/pip.exe" : "bin/pip"
    }

    String toString() {
        return getName().toString()
    }

    PyInstall getInstall(Project project) {
        PyEnv.getEnv(project).getInstall(this)
    }

    Project getProject(Project project) {
        getInstall(project).project
    }

    String taskName(String taskName) {
        return ":$projectPath:$taskName-$name".toString()
    }
}
