import com.jetbrains.python.envs.PythonEnvsExtension
import groovy.transform.CompileStatic
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Exec
import org.gradle.api.tasks.Sync

import static PythonConstants.*
import static PythonVersion.BUILD_DH
import static PythonVersion.BUILD_JUPYTER_GRID

/**
 * Represents an installation of a given version of python.
 *
 * There will be (currently 3) gradle projects per {@link PythonVersion}:
 * :py36, :py37

 * The installations are generated in / expected to be found in:
 * $rootDir/py/installs/(py36|py37).
 *
 * The files themselves are a standard python installation;
 * you probably shouldn't do anything with / to them directly, and instead use a {@link Venv} to install packages into.
 *
 * Each project is configured by this class, which represents everything we might do with a given installation:
 * build a wheel,
 * publish a wheel,
 * require a built wheel,
 * require a published wheel,
 * create java tests with pythonpath (actually, jpy.props),
 * create python tests with java classpath,
 * etc.
 *
 *
 */
@CompileStatic
class PyInstall {

    static final String EXT_NAME = 'install'

    final PyEnv env
    final Project project
    final PythonVersion pv
    final Map<VenvType, Venv> venvs = [:] as LinkedHashMap
    final Exec buildJpyWheel
    boolean finalized

    /**
     * The extension object for the com.jetbrains.python.envs plugin.
     */
    final PythonEnvsExtension envs

    final Map<PySpecType, Sync> generateRequirementsTasks = [:] as LinkedHashMap

    PyInstall(PyEnv env, PythonVersion pv) {
        // don't actually store the project passed in, just use it to find the one-and-only PyInstall
        project = env.rootProject.findProject(pv.getProjectPath())
        this.env = env
        // project.extensions.add, below, will blow up if it's called more than once, which we want.
        // Do not call this constructor more than once!
        // extensions.add() also exposes this object to the gradle project, in case you want to query around:
        // someTask.doFirst { project(':py27').install.doSomethingAtTaskExecutionTime() }
        // Really though, instead of getting this object from gradle project, just use the PyEnv class:
        // PyEnv.getEnv(project).getInstall(PythonVersion.PY_27)
        project.extensions.add(EXT_NAME, this)
        this.pv = pv

        // windows, for some reason, needs us to create these directories up front
        project.buildDir.mkdirs()

        // apply the python-installer plugin
        project.plugins.apply('com.jetbrains.python.envs')

        // Setup basic python installation for each version.
        envs = project.extensions.getByType(PythonEnvsExtension)
        envs.with {
            // where to put installations
            bootstrapDirectory = installRootDir
            // where to put virtual environments
            envsDirectory = venvDir(this)

            // The python version we want
            python pv.name, pv.version, pv.bits
            pipInstallOptions = PyEnv.pipInstallFlags(project)
        }

        Project jpy = project.project(':deephaven-jpy')
        buildJpyWheel = getVenv(VenvType.BUILD_JPY).taskBuildWheel(jpy, jpy.projectDir, 'wheel-jpy')

        PySpec.values()
            .findAll { it.version == pv }
            .each {
                // if we are staging an RPM, we need to make sure the requirements.txt has been generated
                Sync task = it.createGenerateRequirementsTask(project)
                generateRequirementsTasks.put(it.type, task)
            }
    }

    /**
     * Creates the Exec task to construct the deephaven wheel.
     *
     * This should only be called once, and only for the PythonVersion.BUILD_DH version.
     *
     * @return the Exec task created.
     */
    Exec createTaskBuildDeephaven() {
        assert BUILD_DH == pv : "You should only call createTaskBuildDeephaven on $BUILD_DH.projectPathForDebug, not $project.path"
        // creates a Venv, which will register all necessary tasks.
        Venv venv = getVenv VenvType.BUILD_DH

        Project integrations = project.project(':Integrations')
        // Because bdist_wheel pollutes the source directories with `*.egg-info` files,
        // we will copy the python sources to a staging directory before running setup.py in $buildDir/stageDeephaven
        Sync staging = project.tasks.create 'stageDeephaven', Sync, {
            Sync s ->
                s.destinationDir = new File(project.buildDir, 'stageDeephaven')
                s.from(integrations.file('python')){
                    CopySpec c ->
                        c.exclude 'build'
                }
                return
        }

        // Create task to build the wheel (and publish it)
        Exec wheel = venv.taskBuildWheel(project, staging.destinationDir, taskNameBuildDhWheel)
        // Make sure the documentation is correct
        wheel.description = 'Produces any-arch deephaven wheel'
        // Add the task dependency for our staging-sync
        wheel.dependsOn staging

        return wheel
    }

    /**
     * Creates the Exec task to construct the deephaven jupyter grid wheel.
     *
     * This should only be called once, and only for the PythonVersion.BUILD_JUPYTER_GRID version.
     *
     * @return the Exec task created.
     */
    Exec createTaskBuildDeephavenJupyterGrid() {
        assert BUILD_JUPYTER_GRID == pv : "You should only call createTaskBuildDeephavenJupyterGrid on $BUILD_JUPYTER_GRID.projectPathForDebug, not $project.path"
        // creates a Venv, which will register all necessary tasks.
        Venv venv = getVenv VenvType.BUILD_JUPYTER_GRID

        Project jupyterGrid = project.project(':web-jupyter-grid')

        // Because bdist_wheel pollutes the source directories with `*.egg-info` files,
        // we will copy the python sources to a staging directory before running setup.py in $buildDir/stageDeephavenJupyterGrid
        Sync staging = project.tasks.create 'stageDeephavenJupyterGrid', Sync, {
            Sync s ->
                s.destinationDir = new File(project.buildDir, 'stageDeephavenJupyterGrid')
                s.description = "Stage into ${s.destinationDir}"

                // This sets up inputs from a (deferred) task output which will add a dependsOn for us
                s.from(jupyterGrid.provider({
                    jupyterGrid.tasks.named('jupyterGridJs').get().outputs.files
                })) {
                    CopySpec c ->
                        c.into 'deephaven_jupyter_grid'
                        c.exclude 'dist'
                }
                s.from(jupyterGrid.projectDir) {
                    CopySpec c ->
                        c.exclude jupyterGrid.buildDir.name, 'js', 'scripts'
                }
                return
        }

        // Create task to build the wheel (and publish it)
        Exec wheel = venv.taskBuildWheel(project, staging.destinationDir, taskNameBuildJupyterGridWheel)
        // Make sure the documentation is correct
        wheel.description = 'Produces any-arch jupyter grid widget wheel'
        // Add the task dependency for our staging-sync
        wheel.dependsOn staging

        return wheel
    }

    /**
     * Create or return a Venv controller object for the specific {@link VenvType)
     * @param type The VenvType to create a Venv controller object for
     * @return a {@link Venv} object, for creating Exec/JavaExec/etc tasks.
     */
    Venv getVenv(VenvType type) {
        return venvs.computeIfAbsent(type, {
            Venv env = new Venv(this, pv, type)
            // tell the jetbrains plugin to setup our virtual environment.
            envs.virtualenv pv.getVenvName(type), pv.name, type.pypiDependencies
            if (finalized) {
                throw new GradleException("You must call PyInstall.getVenv sooner!\n" +
                        "(cannot be called in gradle.projectsEvaluated or project.afterEvaluate)\n" +
                        "You may need to add evaluationDependsOn ':my-failing-project' in ${project.buildFile.toURI()}")
                // If you are here because this exception was thrown, you need to move
                // the code where you called .getVenv() either into the bottom of this method,
                // or into one of the methods that is already calling getVenv(), either in PyInstall or PyEnv.
                // Due to the way the plugin we depend on is setup, there is a cutoff after which new venvs cannot be declared.
            }
            env.installShowInstalled()
            env.installShowOutdated()
            return env
        })
    }

    /**
     * @return The root directory where python installations will be placed.  Defaults to py/installs
     */
    File getInstallRootDir() {
        String configured = project.findProperty(PROP_INSTALL_ROOT)
        return configured ?
                // if we supply -Ppy.install.root=some/dir, it will be parsed as a directory relative to iris repo root directory
                // absolute paths will be treated as absolute, so you can freely set py.install.root to any *existing* path on host machine
                project.rootProject.file(configured) :
                // default location is $rootDir/py/installs
                new File(project.rootDir, DEFAULT_INSTALL_ROOT)
    }

    String getDeephavenJpyWheelRpmPath() {
        "${pv.rpmPath}/wheels"
    }

    FileCollection getDeephavenJpyWheelRpmFiles() {
        buildJpyWheel.outputs.files
    }

    private void addTaskDependency(String task, String dependsOn) {
        project.tasks.getByName(task).dependsOn project.tasks.getByName(dependsOn)
    }

    /**
     * Perform all per-python-version initialization.
     *
     * This is used to setup jpy build tasks, jpy test tasks and jpy+dh test tasks.
     *
     * All tasks to create and install jpy wheels will be setup here.
     *
     * If you are adding new python wheels, you probably want to do your test setup here;
     * any arch-independent wheels should be setup in {@link PyEnv#initializeEnvironments()}
     */
    void initialize() {
        // Setup per-python-version virtual environments.
        // We setup buildJpyWheel so we can assign the Exec to a final variable.
        Exec runJpy = getVenv(VenvType.RUN_JPY).installJpyWheel()
        Exec testJpy = getVenv(VenvType.TEST_JPY).installJpyWheel()
        Exec testDh = getVenv(VenvType.TEST_DH).installDhAndDhJpyWheels()
        Exec runDh = getVenv(VenvType.RUN_DH).installDhAndDhJpyWheels()
        if (generateRequirementsTasks.containsKey(PySpecType.WORKER_JUPYTER)) {
            // there is probably a cleaner way to model this, but we *can't* install jupyter
            // in py27 b/c we don't support that environment.
            Exec runDhJupyter = getVenv(VenvType.RUN_DH_JUPYTER).installDhJupyterWorkerWheels()
            Exec runDhJupyterNotebookServer = getVenv(VenvType.RUN_JUPYTER_NOTEBOOK_SERVER).installDhJupyterNotebookServer()
        }

        // some parallel performance optimizations
        testDh.shouldRunAfter(testJpy)
        runJpy.shouldRunAfter(testDh)

        if (pv == PythonVersion.PY_37) {
            // We need this for project(':notebook'). Since ':notebook's evaluation depends on
            // ':py37', ':notebook' won't be able to initialize the venv, so we must do it here.
            getVenv(VenvType.RUN_NBFORMAT)
        }

        // Add a callback to execute after the gradle project has been "evaluated";
        // there is no .gradle buildscript for `:pyNN` projects to evaluate,
        // but we still need to wait for a project.afterEvaluate call in the "com.jetbrains.python.envs" plugin to execute.
        project.afterEvaluate this.&finalizeTasks
    }

    /**
     * Called after the project has evaluated, and the plugin we depends on has finally created the venv/bootstrap tasks
     */
    void finalizeTasks() {
        venvs.keySet().each this.&finalizeEnv
        // setting finalized to true will cause any venv created after this to blow up.  As it likely should.
        // The python plugin we depend on uses an afterEvaluate instead of reactive programming,
        // so any venv created after now will be missing task-setup from the plugin.  Better to fail loudly when this happens.
        finalized = true
    }

    /**
     * Makes the create venv tasks depend on the create python installation tasks.
     *
     * The python plugin *should* be creating these dependency links, but it's not...
     */
    void finalizeEnv(VenvType envName) {
        addTaskDependency pv.getCreateVenvTaskName(envName), pv.bootstrapTaskName
    }
}
