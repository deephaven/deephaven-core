import groovy.transform.CompileStatic
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.internal.os.OperatingSystem

/**
 * A place to keep statically-available python constants
 */
@CompileStatic
final class PythonConstants {

    /**
     * Where we, by default, put our python installations.
     *
     */
    static final String DEFAULT_INSTALL_ROOT = 'py/installs'
    /**
     * The gradle.properties key to override the location where python installations can be found.
     *
     * We expect / will create if missing, the following directory structure:
     * File root = new File(findProperty(PROP_INSTALL_ROOT) ?: DEFAULT_INSTALL_ROOT)

     * "$root/py36" -> a python 3.6 installation
     * "$root/py37" -> a python 3.7 installation
     *
     * These installations will be treated as gradle projects / intellij modules,
     * but they will not have configurable .gradle files.
     *
     * Instead, these projects will be configured-on-demand, by code in buildSrc,
     * when projects which actually use the installation to do something, need something.
     *
     * We need to be able to delete these directories entirely and not lose anything valuable,
     * so they are instead configured by the {@link PyEnv} class.
     *
     */
    static final String PROP_INSTALL_ROOT = 'py.install.root'

    static final String wheelPrefixJupyterBootstrap = 'deephaven_python_bootstrap_kernel'
    static final String wheelPrefixJupyterGrid = 'deephaven_jupyter_grid'
    static final String venvBuildJpy = 'build-jpy'
    static final String venvRunJpy = 'run-jpy'
    static final String venvTestJpy = 'test-jpy'
    static final String venvTestDh = 'test-dh'
    static final String venvRunDh = 'run-dh'
    static final String venvRunDhJupyter = 'run-dh-jupyter'
    static final String venvRunDhJupyterNotebookServer = 'run-dh-jupyter-notebook-server'
    static final String venvBuildDh = 'build-deephaven'
    static final String venvBuildJupyterGrid = 'build-jupyter-grid'
    static final String venvTestJupyterGrid = 'test-jupyter-grid'
    static final String venvPyDocs = 'py-docs'
    static final String venvRunNbformat = 'run-nbformat'
    static final String venvSphinx = 'run-sphinx'
    static final String taskNameBuildDhWheel = 'wheel-dh'
    static final String taskNameBuildJupyterGridWheel = 'wheel-jupyter-grid'

    static final boolean INCLUDE_IN_RPM = true
    static final boolean EXCLUDE_FROM_RPM = false

    static File wheelDirDh(Project project) {
        File buildDir = project.project(':Integrations').buildDir
        return new File(buildDir, "dist-${project.version}")
    }

    static File wheelDirDhJupyterGrid(Project project) {
        File buildDir = project.project(':web-jupyter-grid').buildDir
        return new File(buildDir, "dist-${project.version}")
    }

    static File venvDir(Project project) {
        return new File(project.rootDir, 'py/venvs')
    }

    static File venvDir(PyInstall install) {
        return new File(install.project.rootDir, 'py/venvs')
    }

    static boolean isWindows() {
        return OperatingSystem.current().isWindows()
    }

    static String getJpy_platform_name() {
        switch (OperatingSystem.current()) {
            case OperatingSystem.LINUX:
                return "x86_64-linux-gnu"
            case OperatingSystem.MAC_OS:
                return "darwin"
            case OperatingSystem.WINDOWS:
                // windows does not have arch in the .pyd filenames that this var is used for (they are always jpy.pyd | jdl.pyd).
                // see https://docs.python.org/3/faq/windows.html#is-a-pyd-file-the-same-as-a-dll for more information about pyd
                return "not_relevant"
        }
        throw new GradleException("Can't get platform name for " + OperatingSystem.current())
    }

}
