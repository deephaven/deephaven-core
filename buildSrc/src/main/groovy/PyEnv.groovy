import groovy.transform.CompileStatic
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.file.FileCollection
import org.gradle.api.internal.project.ProjectInternal
import org.gradle.api.tasks.Exec
import org.gradle.internal.nativeintegration.ProcessEnvironment
import org.gradle.internal.os.OperatingSystem

import javax.inject.Inject

/**
 * A utility class for initializing / interfacing with gradle-generated python environments.
 *
 * This object represents each project's python-related-task needs, and installs all tasks necessary
 * to build wheels, run java tests with pythonpath, run python tests with classpath, and contribute code to our rpm output.
 *
 * Each project which does pythonic things should get a PyEnv instance from {@link #getEnv}.
 * There will be `PyEnv pyEnv` object attached to the root gradle project, but you should not rely on this fact,
 * as it is installed there when calling `PyEnv.getEnv(project)` (<- how you should get the one-and-only instance of PyEnv)
 */
@CompileStatic
class PyEnv {

    /**
     * The variable name "pyEnv", attached to rootProject.extensions after any project which invokes {@link #getEnv(Project)}.
     *
     * To find in java/well-typed-groovy:
     * rootProject.extensions.findByName(PyEnv.EXT_NAME) as PyEnv
     * or
     * rootProject.extensions.findByType(PyEnv.class)
     * or, in dynamic groovy / gradle:
     * rootProject.pyEnv.someMethod()
     * or (if calling code has a root scope in the build.gradle file)
     * pyEnv.someMethod()
     *
     */
    public static final String EXT_NAME = "pyEnv"

    /**
     * A map of every {@link PyInstall}, one for each {@link PythonVersion} that we support.
     */
    private final Map<PythonVersion, PyInstall> installs = [:] as LinkedHashMap

    /**
     * A gradle property to bypass hand-holding w.r.t. missing zlib
     */
    private static final String IGNORE_MISSING_ZLIB = 'ignore.missing.zlib'

    /**
     * The rootProject, so we can call rootProject.project(':path') to find other projects.
     */
    final Project rootProject

    /**
     * The task to build the deephaven wheel.  Exposed so you can easily call myTask.dependsOn pyEnv.buildDhWheel
     * or, for any copy/sync/rpm task:
     * myTask.from pyEnv.buildDhWheel.outputs.files
     * The above syntax will add the wheel to a copy task, AND hook up the myTask.dependsOn pyEnv.buildDhWheel for you.
     */
    Exec buildDhWheel

    Exec buildDhJupyterGridWheel

    @Inject
    PyEnv(Project p) {
        this.rootProject = p
        assert p == p.rootProject
    }

    /**
     * This method is the entry point for any project who wishes to use any python functionality.
     *
     * Once this method has returned, all basic tasks for initializing python environments has been setup.
     *
     * Note that tasks from the jetbrains python plugin may not exist yet,
     * but you should not really be accessing them directly.  Instead use methods in {@link Venv} to do so.
     *
     * @param p Any gradle project
     * @return a PyEnv instance, for accessing {@link PyInstall} objects which expose {@link Venv} objects.
     */
    static PyEnv getEnv(Project p) {
        p = p.rootProject
        PyEnv pyEnv = p.extensions.findByName(EXT_NAME) as PyEnv
        if (!pyEnv) {
            pyEnv = p.extensions.create(EXT_NAME, PyEnv, p)
            assert new PyEnv(p) != pyEnv : ''
            // ^^ just here in case you want to trace the constructor to above extensions.create() call
            ProcessEnvironment proc = (p as ProjectInternal).services.get(ProcessEnvironment) as ProcessEnvironment
            // rather than telling the user to set this environment variable, we will set it for them.
            proc.setEnvironmentVariable('PYTHON_CONFIGURE_OPTS', '--enable-shared')
            if (System.getenv('PYTHON_CONFIGURE_OPTS') != '--enable-shared') {
                throw new GradleException('Must set env PYTHON_CONFIGURE_OPTS to --enable-shared')
            }

            if (OperatingSystem.current().isMacOsX() && 'true' != p.findProperty(IGNORE_MISSING_ZLIB) ) {
                // Mac also needs additional environment variables / packages installed.
                String zlib = ['brew', '--prefix', 'zlib'].execute().text.trim()
                if (!new File(zlib).directory) {
                    throw new GradleException("""zlib is required, but $zlib does not exist.
Run `brew install zlib` and `xcode-select --install` to repair.

If you wish to attempt to install python w/out recommended configuration,
pass -P$IGNORE_MISSING_ZLIB=true to bypass this exception""")
                }
                String cppflags = System.getenv('CPPFLAGS')
                if (!cppflags || !cppflags.contains("-I$zlib/include")) {
                    cppflags = "${(cppflags?"$cppflags ":'')}-I$zlib/include"
                    proc.setEnvironmentVariable('CPPFLAGS', cppflags)
                }
            }

            // TODO: remove the code below once we merge the removal of mvn: IDO-438
            try {
                String mvn = (OperatingSystem.current().isWindows() ? ['cmd', '/c', 'mvn', '-v'] : ['mvn', '-v']).execute().text.trim()
                if (!mvn) {
                    throw new Error()
                }
            } catch(Throwable e) {
                throw new GradleException("""We currently require maven (mvn) to be on your \$PATH in order to build jpy.
    ${OperatingSystem.current().isMacOsX() ? 'Please run `brew install maven`' : 'Download maven and update your PATH to include $M2_HOME/bin'}""")
            }

            pyEnv.initializeEnvironments()
        }
        return pyEnv
    }

    static String pipInstallFlags(Project project) {
        File cacheDir = project.rootProject.file(project.property('pipCacheDir'))
        cacheDir.mkdirs()
        project.logger.debug("Using $cacheDir as pip cache dir for $project.path")
        return "--retries=10 --timeout=30 --cache-dir=${cacheDir.absolutePath}"
    }
/**
     * Setup all python wheel build + install tasks.
     */
    private void initializeEnvironments() {

        // Create one PyInstall per PythonVersion enum member
        for (PythonVersion pv : PythonVersion.values()) {
            // PyInstall constructor sets up bootstrap tasks for creating python installs
            PyInstall install = new PyInstall(this, pv)
            def was = installs.put(pv, install)
            assert was == null : "Do not call PyEnv.initializeEnvironments more than once! Had non-null $pv in $installs"
        }

        // Next, we create the universal "build dh wheel".
        // Tasks in PyInstall might depend on this, so the order of method calls here is important;
        // this *must* be done before PyInstall.initialize(), below.
        buildDhWheel = installs.get(PythonVersion.BUILD_DH).createTaskBuildDeephaven()

        buildDhJupyterGridWheel = installs.get(PythonVersion.BUILD_JUPYTER_GRID).createTaskBuildDeephavenJupyterGrid()

        PyInstall prev = null
        for (PyInstall install : installs.values()) {
            // Setup per-python-version tasks, mostly setting up venvs.
            install.initialize()
            // make sure jpy wheel building takes turns; we can fail in --parallel w/out this.
            if (prev) {
                // we can't let the wheel building tasks run at the same time;
                // bdist_wheel does not support parallel execution;
                // we _could_ sync the source into a build directory, but not worth it for now.
                install.buildJpyWheel.mustRunAfter prev.buildJpyWheel
            }
            prev = install
        }

    }

    /**
     * @param p Any project, for looking up properties.
     * @return true if python is enabled on this system.
     *
     * For legacy compatibility, we'll look for environment variable BUILD_PY=true
     * Preferred method of enabling python is the gradle property `-PwithPy=true`.
     * You may set this property in ~/.gradle/gradle.properties if you want python always-on for every clone (and in IDE).
     * You may pass it on command line -PwithPy=true
     * You may set it in intellij gradle settings under `Gradle VM options` via `-Dorg.gradle.project.withPy=true`
     *
     * Setting withPy=true via intellij gradle settings / global gradle properties
     * is required for python tasks to be displayed in gradle menu.
     *
     */
    static boolean pythonEnabled(Project p) {
        return System.getenv('BUILD_PY') == 'true' || p.findProperty('withPy') == 'true'
    }

    PyInstall getInstall(PythonVersion version) {
        return installs.get(version)
    }

    Venv getVenv(PythonVersion version, VenvType type) {
        return getInstall(version).getVenv(type)
    }

    Exec getTaskBuildDhWheel() {
        assert buildDhWheel != null : "wheel-dh task not initialized yet"
        return buildDhWheel
    }

    Exec getTaskBuildDhJupyterGridWheel() {
        assert buildDhJupyterGridWheel != null : "DhJupyterGridWheel task not initialized yet"
        return buildDhJupyterGridWheel
    }

    String getUniversalWheelRpmPath() {
        "python/wheels"
    }

    String getOldWheelRpmPath() {
        "wheels"
    }

    FileCollection getDeephavenWheelRpmFiles() {
        getTaskBuildDhWheel().outputs.files
    }

    FileCollection getDeephavenJupyterGridWheelRpmFiles() {
        getTaskBuildDhJupyterGridWheel().outputs.files
    }
}
