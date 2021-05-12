import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.Configuration
import org.gradle.api.invocation.Gradle
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.*
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.util.PatternFilterable
import org.gradle.internal.jvm.Jvm
import org.gradle.language.base.plugins.LifecycleBasePlugin

/**
 * This class is a "venv controller" class, and is used to setup tasks that will
 * install wheels into the venv, or create Exec/JavaExec tasks to be able to
 * run python in a given venv, or run java w/ jpy setup on the given venv.
 */
@CompileStatic
class Venv {

    final PythonVersion pv
    final VenvType type
    final PyInstall install

    Venv(PyInstall install, PythonVersion pv, VenvType type) {
        this.install = install
        this.pv = pv
        this.type = type
        // whenever a project accesses a Venv object, add an entry to it's clean task to delete the venv.
        (install.project.tasks.getByName('clean') as Delete).delete dir
    }

    String getVenvTypeName() {
        return type.venvName
    }

    String getVenvFullName() {
        return pv.getVenvName(type)
    }

    String getDir() {
        return "$baseDir/$venvFullName"
    }

    File getBaseDir() {
        return PythonConstants.venvDir(install)
    }

    String getPip() {
        return "$dir/$pv.execPip"
    }

    String getPython() {
        return "$dir/$pv.execPy"
    }

    String getProjectPath() {
        return pv.projectPath
    }

    String getTaskNameCreateVenv() {
        return "$install.project.path:${pv.getCreateVenvTaskName(type)}"
        // install.taskCreateVenv(VenvType.TEST_JPY)
    }

    String getTaskNameInstallWheel() {
        return ":$projectPath:$taskNameInstallWheelInternal"
    }

    String getTaskNameProcessResources() {
        return ":$projectPath:processResources"
    }

    /**
     * @return The task name to use to install wheels into this venv.
     *
     * There should only be one such task per venv.
     * use `doLast { project.exec {} }` to call pip more than once, if needed.
     */
    String getTaskNameInstallWheelInternal() {
        return "install-wheel-$venvTypeName"
    }

    String getTaskNameInstallGridWheelInternal() {
        return 'installGridWheel'
    }

    String getWheelDir() {
        String v = install.project.version.toString()
        switch (type) {
            case VenvType.BUILD_DH:
                return "${project(':Integrations').buildDir}/dist-$v".toString()
            case VenvType.BUILD_JUPYTER_GRID:
                return "${project(':web-jupyter-grid').buildDir}/dist-$v".toString()
        }
        // todo: should we invert these conditions, so deephaven-jpy.buildDir is more selective
        // instead of the default?
        return pv.getWheelDir(project(':deephaven-jpy').buildDir, v, type, pv)
    }

    private Project project(String p) {
        return install.project.project(p)
    }

    void applyProperties(Test t) {
        t.systemProperties pv.getRunProperties(install.installRootDir, baseDir, type)
    }

    void applyProperties(JavaExec t) {
        t.systemProperties pv.getRunProperties(install.installRootDir, baseDir, type)
    }

    /**
     * Get or create a task to build a wheel file.
     *
     * The created task is automatically setup for publishing during the publishPython task.
     * If we create any wheel we don't want pushed to artifactory for some reason,
     * then just add a `boolean publish=false` parameter to skip the publishing logic at end of method.
     *
     * @param p the project to install the task into
     * @param workingDir the source directory containing a setup.py file
     * @param wheelName the "name" of the wheel, like `deephaven` or `deephaven-jpy`
     * @return an Exec task to build the wheel.
     */
    Exec taskBuildWheel(Project p, File workingDir, String wheelName) {
        TaskContainer tasks = install.project.tasks
        String taskName = "$wheelName-${pv.name}".toString()
        Task existing = tasks.findByName(taskName)
        if (existing) {
            return existing as Exec
        }

        Exec exe = tasks.create(taskName, Exec)
        exe.dependsOn taskNameCreateVenv
        taskWheel(p).dependsOn(exe)

        exe.setGroup 'python'

        // hm.  this is setting the venv directory as an input to building the wheel... maybe overkill
        exe.inputs.dir dir
        // todo IDO-289
        exe.inputs.property('version', pv)
        exe.inputs.files(p.fileTree(workingDir).matching {
            PatternFilterable f ->
                // bdist_wheel is not configurable, it will always pollute the working directory
                f.exclude 'build', 'target', 'lib', '*.egg-info',
                        '__pycache__', 'jpyutil.pyc', 'setup.out'
        })

        exe.workingDir workingDir
        exe.outputs.dir(wheelDir)

        // it's really tough to pass parameter via command line to setup.py
        // https://stackoverflow.com/questions/677577/distutils-how-to-pass-a-user-defined-parameter-to-setup-py
        // we'll use env variable
        exe.environment 'DEEPHAVEN_VERSION', p.version

        exe.commandLine python, 'setup.py', 'bdist_wheel', '-d', wheelDir

        exe.doLast {
            File wheelDir = new File(getWheelDir())
            File wheelParent = wheelDir.getParentFile()
            if (wheelDir.name.startsWith("dist-")) {
                FilenameFilter filter = { File dir, String maybeStale ->
                    maybeStale.startsWith("dist-") && maybeStale != wheelDir.name
                }
                wheelParent.listFiles(filter).each{
                    File stale ->
                        exe.logger.quiet "Deleted stale wheel directory {}? {}", stale,
                            stale.deleteDir()
                }
            } else {
                exe.logger.quiet("Unsupported wheel directory name $wheelDir, expected last path segment to start with `dist-`")
            }
        }

        return exe
    }

    /**
     * Get or create a task to install deephaven and deephaven-jpy into the venv this object represents.
     */
    Exec installDhAndDhJpyWheels() {
        Project p = project(projectPath)
        TaskContainer tasks = p.tasks
        Exec e = tasks.maybeCreate(taskNameInstallWheelInternal, Exec)
        if (e.group != 'python') {
            def specType = PySpecType.WORKER_STANDARD

            e.group = 'python'
            e.description = "Install '${specType.type}' requirements into the local venv (${venvFullName})"
            e.dependsOn install.env.taskBuildDhWheel, install.buildJpyWheel, taskNameCreateVenv, install.generateRequirementsTasks.get(specType)

            // pip install --find-links=/paths/to/wheels --requirements=requirements.txt --retries=10 --timeout=30 --cache-dir=cache-dir

            List instructions = [
                pip,
                'install',
                "--find-links=${install.getVenv(VenvType.BUILD_JPY).wheelDir}",
                "--find-links=${PythonConstants.wheelDirDh(p)}",
                "--requirement=${specType.getGeneratedRequirementsPath(p)}"
            ]
            instructions.addAll(PyEnv.pipInstallFlags(p).split(" "))

            e.commandLine instructions
        }
        return e
    }

    /**
     * Get or create a task to install the jpy wheel into the venv this object represents
     */
    Exec installJpyWheel() {
        Project p = project(projectPath)
        TaskContainer tasks = p.tasks
        Exec e = tasks.maybeCreate(taskNameInstallWheelInternal, Exec)
        if (e.group != 'python') {
            def specType = PySpecType.JPY_ONLY

            e.group = 'python'
            e.description = "Install '${specType.type}' requirements into the local venv (${venvFullName})"
            e.dependsOn install.env.taskBuildDhWheel, install.buildJpyWheel, taskNameCreateVenv, install.generateRequirementsTasks.get(specType)

            // pip install --find-links=/paths/to/wheels --requirements=requirements.txt --retries=10 --timeout=30 --cache-dir=cache-dir

            List instructions = [
                pip,
                'install',
                '--ignore-installed',
                "--find-links=${install.getVenv(VenvType.BUILD_JPY).wheelDir}",
                "--requirement=${specType.getGeneratedRequirementsPath(p)}"
            ]
            instructions.addAll(PyEnv.pipInstallFlags(p).split(" "))

            e.commandLine instructions

        }
        return e

    }

    /**
     * Get or create a task to install deephaven jupyter worker env into the venv this object represents.
     */
    Exec installDhJupyterWorkerWheels() {
        Project p = project(projectPath)
        TaskContainer tasks = p.tasks
        Exec e = tasks.maybeCreate(taskNameInstallWheelInternal, Exec)
        if (e.group != 'python') {
            def specType = PySpecType.WORKER_JUPYTER

            e.group = 'python'
            e.description = "Install '${specType.type}' requirements into the local venv (${venvFullName})"
            e.dependsOn install.env.taskBuildDhWheel,
                install.buildJpyWheel,
                install.env.taskBuildDhJupyterGridWheel,
                taskNameCreateVenv,
                install.generateRequirementsTasks.get(specType)

            // pip install --find-links=/paths/to/wheels --requirements=requirements.txt --retries=10 --timeout=30 --cache-dir=cache-dir

            List instructions = [
                pip,
                'install',
                "--find-links=${install.getVenv(VenvType.BUILD_JPY).wheelDir}",
                "--find-links=${PythonConstants.wheelDirDh(p)}",
                "--find-links=${PythonConstants.wheelDirDhJupyterGrid(p)}",
                "--requirement=${specType.getGeneratedRequirementsPath(p)}"
            ]
            instructions.addAll(PyEnv.pipInstallFlags(p).split(" "))

            e.commandLine instructions
        }
        return e
    }

    /**
     * Get or create a task to install deephaven jupyter server env into the venv this object represents.
     */
    Exec installDhJupyterNotebookServer() {
        Project p = project(projectPath)
        TaskContainer tasks = p.tasks
        Exec e = tasks.maybeCreate(taskNameInstallWheelInternal, Exec)
        if (e.group != 'python') {
            def specType = PySpecType.JUPYTER_NOTEBOOK_SERVER

            e.group = 'python'
            e.description = "Install '${specType.type}' requirements into the local venv (${venvFullName})"
            e.dependsOn install.env.taskBuildDhJupyterGridWheel, taskNameCreateVenv, install.generateRequirementsTasks.get(specType)

            // todo: point to grid widget extension wheels eventually

            // pip install --requirements=requirements.txt --retries=10 --timeout=30 --cache-dir=cache-dir

            List instructions = [
                pip,
                'install',
                "--find-links=${PythonConstants.wheelDirDhJupyterGrid(p)}",
                "--requirement=${specType.getGeneratedRequirementsPath(p)}"
            ]
            instructions.addAll(PyEnv.pipInstallFlags(p).split(" "))

            e.commandLine instructions
        }
        return e
    }

    /**
     * Get or create a task that outputs the results of the venv's `pip freeze`
     */
    Exec installShowInstalled() {
        Project p = project(projectPath)
        TaskContainer tasks = p.tasks
        Exec e = tasks.maybeCreate("show-$venvTypeName", Exec)
        if (e.group != 'python') {
            e.group = 'python'
            e.description = "Show the installed packages of the local venv (${venvFullName})"
            e.dependsOn taskNameCreateVenv

            // Note: *NOT* depending on the install wheel task - this task is more for ad-hoc debugging,
            // and not a declarative state meant to show the "final" state of the virtual env.
            // As such, it explicitly has the minimal requirements required for executing pip freeze.
            // These semantics *may* change in the future when/if we automate requirement updating.
            //e.dependsOn taskNameInstallWheelInternal

            e.commandLine pip, 'freeze'
        }
        return e
    }

    /**
     * Get or create a task that outputs the results of the venv's `pip list --outdated`
     */
    Exec installShowOutdated() {
        Project p = project(projectPath)
        TaskContainer tasks = p.tasks
        Exec e = tasks.maybeCreate("outdated-$venvTypeName", Exec)
        if (e.group != 'python') {
            e.group = 'python'
            e.description = "Show the outdated packages of the local venv (${venvFullName})"
            e.dependsOn taskNameCreateVenv

            // Note: *NOT* depending on the install wheel task - this task is more for ad-hoc debugging,
            // and not a declarative state meant to show the "final" state of the virtual env.
            // As such, it explicitly has the minimal requirements required for executing pip list --outdated.
            // These semantics *may* change in the future when/if we automate requirement updating.
            //e.dependsOn taskNameInstallWheelInternal

            e.commandLine pip, 'list', '--outdated'
        }
        return e
    }

    /**
     * Gets or creates a generic "lifecycle task" named `wheel`.
     *
     * This lifecycle task depends on all tasks which generate wheel files.
     *
     * This is used so you can run ./gradlew wheel w/out knowing the names of the tasks to create all wheels.
     *
     */
    Task taskWheel(Project project) {
        Task wheel
        if (project.tasks.names.contains('wheel')) {
            // purposely not using findByName, since that will eagerly resolve all tasks
            wheel = project.tasks.getByName('wheel')
        } else {
            wheel = project.tasks.create('wheel')
            wheel.group = 'python'
            wheel.description = '''Lifecycle tasks for assembling python wheels.'''
            //noinspection UnstableApiUsage
            project.tasks.getByName(LifecycleBasePlugin.ASSEMBLE_TASK_NAME).dependsOn wheel
        }
        return wheel
    }

    /**
     * Get or create a "lifecycle task" named `testPython`.
     *
     * This task will depend on all python-related test classes,
     * so you can run ./gradlew testPython (or, `gw tPy` if you have `gw` alias setup).
     */
    Task taskTestPython(Project project) {
        Task testPython
        if (project.tasks.names.contains('testPython')) {
            // purposely not using findByName, since that will eagerly resolve all tasks
            testPython = project.tasks.getByName('testPython')
        } else {
            testPython = project.tasks.create('testPython')
            testPython.group = 'python'
            testPython.description = '''Lifecycle tasks for all python-related tasks
This task will dependsOn other tasks which actually perform testing.'''
            testPython.onlyIf { TestTools.shouldRunTests(project) }
            //noinspection UnstableApiUsage
            project.tasks.getByName(LifecycleBasePlugin.CHECK_TASK_NAME).dependsOn testPython
        }
        return testPython
    }

    /**
     * Creates a java Test task with classpaths and jpy settings properly applied to be able to access this venv at runtime.
     *
     * Note that task testPython will dependsOn the created task.
     *
     * @param project The project to install the test task into
     * @param taskName The task name to ues; default is testJavaWithPython
     * @param dependOn An object (task or string) to dependsOn.  Default is {@link #installDhAndDhJpyWheels()}.
     * @param classpath The sourceSet to use to find the classpath to use.  Default is null, which uses testRuntimeClasspath.
     * @return A Test task instance, in case you want to setup more dependsOn logic, or perform other tweaks.
     */
    Test javaTest(Project project, CharSequence taskName = 'testJavaWithPython', Object dependOn = installDhAndDhJpyWheels(), SourceSet classpath=null) {

        return project.tasks.create (taskName.toString(), Test) {
            Test t ->
                t.group = 'python'
                t.description = "Run java tests with jpy setup using python environment $venvFullName at file:$dir"
                t.onlyIf { TestTools.shouldRunTests(project) }

                // Cleaning up on a dedicated thread has some issues when there is frequent starting
                // and stopping of the python virtual environment. We'll rely on cleaning up inline
                // when necessary.
                t.systemProperty"PyObject.cleanup_on_thread", "false"

                t.dependsOn dependOn
                taskTestPython(project).dependsOn t

                applyProperties(t)
                if (classpath) {
                    t.classpath = classpath.runtimeClasspath
                    t.testClassesDirs = classpath.output.classesDirs
                }
                return
        }

    }

    /**
     * Create a JavaExec task which has the deephaven-jpy wheels installed, but not the deephaven wheel.
     *
     * @param project The project to install the task into
     * @param taskName The name of the task
     * @param main The name of the main class to invoke.
     * @param classpath Optional: The classpath to use. May be null, which will use default of runtimeClasspath
     * @param cb Optional: A CallBack Action to configure the JavaExec task.  Here only for convenience to calling code.
     * @return A JavaExec task with jpy setup and installed into the venv.
     */
    JavaExec javaExecJpyOnly(Project project, CharSequence taskName, CharSequence main, SourceSet classpath=null, Action<? super JavaExec> cb={}) {
        return javaExec(project, taskName, main, taskNameInstallWheel, classpath, cb)
    }

    /**
     *
     * Create a JavaExec task which, by default has the deephaven and deephaven-jpy wheels installed.
     *
     * @param project The project to install the task into
     * @param taskName The name of the task
     * @param main The name of the main class to invoke.
     * @param dependOn The tasks to dependsOn before running the returned task. Defaults to {@link #installDhAndDhJpyWheels()}
     * @param classpath Optional: The classpath to use. May be null, which will use default of runtimeClasspath
     * @param cb Optional: A CallBack Action to configure the JavaExec task.  Here only for convenience to calling code.
     * @return A JavaExec task with jpy setup and installed into the venv.
     */
    JavaExec javaExec(Project project, CharSequence taskName, CharSequence main, Object dependOn = installDhAndDhJpyWheels(), SourceSet classpath=null, Action<? super JavaExec> cb={}) {

        return project.tasks.create (taskName.toString(), JavaExec) {
            JavaExec t ->
                t.group = 'python'
                t.description = "Run $main with jpy setup using python environment $venvFullName at file:$dir"
                t.main = main

                t.dependsOn dependOn

                applyProperties(t)
                if (classpath) {
                    t.classpath = classpath.runtimeClasspath
                }
                cb.execute(t)
                return
        }

    }

    //TODO core#430 delete when this task is closed out
    Exec pythonExec(Project project,
                    CharSequence taskName,
                    List<String> arguments,
                    Object dependOn=installDhAndDhJpyWheels(),
                    Object workingDir= "${project.projectDir}/python",
                    String classpathConfiguration =null
    ){
        TaskContainer tasks = project.tasks
        Exec exe = tasks.create(taskName.toString(), Exec)
        exe.group = 'python'
        exe.description = "Run python $arguments using environment $venvFullName at file:$dir"

        exe.dependsOn dependOn
        // Set the project version as an input property, so that when the version changes, we will re-run this task.
        exe.inputs.property('version', project.version)

        exe.inputs.files(project.fileTree(workingDir).matching {
            PatternFilterable f ->
                // bdist_wheel is not configurable, it will always pollute the working directory
                f.exclude 'build'
                f.exclude '*.egg-info'
        })

        if (classpathConfiguration){
            String ws = "$project.buildDir/tmp/ws-$venvFullName".toString()
            exe.doFirst {
                new File(ws.toString()).mkdirs()  // consider deep-deleting the dir if it exists, for clean state?
            }

            //noinspection UnstableApiUsage
            Configuration cp = project.configurations.getByName(classpathConfiguration)

            exe.dependsOn(cp)
            // These are only really needed by Integrations/python/test/__init__.py,
            // but we may want to extract that class into a deephaven-test wheel and reuse it.
            exe.environment 'DEEPHAVEN_CLASSPATH', "${->cp.asPath}"
            exe.environment 'DEEPHAVEN_VERSION', project.version
            exe.environment 'DEEPHAVEN_WORKSPACE', ws
            exe.environment 'DEEPHAVEN_DEVROOT', project.rootDir
            exe.environment 'DEEPHAVEN_PROPFILE', 'dh-defaults.prop'
            exe.environment 'JDK_HOME', Jvm.current().javaHome
        }

        exe.workingDir workingDir
        List<String> args = arguments.toList()
        args.add(0, python)
        exe.commandLine args

        return exe
    }

    /**
     * Creates an Exec task which invokes python unittests using this Venv.
     *
     * Note that the testPython will .dependsOn the returned task.
     *
     * @param project The project to install the task into
     * @param taskName The name of the task to install. Default is testPythonWithJava
     * @param dependOn A task to dependsOn.  Defaults to {@link #installDhAndDhJpyWheels()}
     * @param workingDir The working directory in which to execute the task; should be the directory root of python sources.
     * @return An Exec task which will run the tests.
     */
    //TODO core#430 delete when this task is closed out
    Exec pythonTest(Project project,
                    CharSequence taskName = 'testPythonWithJava',
                    Object dependOn = installDhAndDhJpyWheels(),
                    Object workingDir = "${project.projectDir}/python") {
        String reportDir = "$project.buildDir/test-results/$venvFullName".toString()
        // Invoke xmlrunner in discover mode, which will find all unittest classes in the ./test subdirectory.
        // Outputs junit-style xml to the reportDir.
        Exec e = pythonExec(project, taskName, ['-m', 'xmlrunner', 'discover', '-v', '-o', reportDir],
                dependOn, workingDir, JavaPlugin.TEST_RUNTIME_CLASSPATH_CONFIGURATION_NAME)

        e.onlyIf { TestTools.shouldRunTests(project) }
        taskTestPython(project).dependsOn e

        // validate that test discovery location exists
        if (!new File(e.workingDir, 'test')) {
            throw new GradleException("Python unittest convention is to place tests in \$pythonSourceDir/test directory, " +
                    "which is not found in $workingDir")
        }

        // empty output location
        e.doFirst {
            // clean out the reporting directory before we run.
            project.delete(reportDir)
        }
        return e
    }
}
