import static PythonConstants.venvBuildDh
import static PythonConstants.venvBuildJpy
import static PythonConstants.venvBuildJupyterGrid
import static PythonConstants.venvRunDh
import static PythonConstants.venvRunDhJupyter
import static PythonConstants.venvRunDhJupyterNotebookServer
import static PythonConstants.venvRunJpy
import static PythonConstants.venvSphinx
import static PythonConstants.venvTestDh
import static PythonConstants.venvTestJpy
import static PythonConstants.venvTestJupyterGrid
import static PythonConstants.venvPyDocs
import static PythonConstants.venvRunNbformat

class VenvTypeConstants {
    // IDS-5998
    // unittest-xml-reporting stopped supporting python 2 after 2.5.2, but they didn't update their compatibility matrix properly.
    // https://github.com/xmlrunner/unittest-xml-reporting/issues/213
    // This commit fixes unittest-xml-reporting, https://github.com/xmlrunner/unittest-xml-reporting/commit/715f05738fb21c53802282ca23f7cb70a726ac9a,
    // but I'm not sure if it retroactively applies - and either way, we don't want to wait for a new release from them.
    static final List<String> TEST_DEPS = ['unittest2', 'unittest-xml-reporting==2.5.2']
}

enum VenvType {
    BUILD_DH(venvBuildDh),
    BUILD_JPY(venvBuildJpy),
    BUILD_JUPYTER_GRID(venvBuildJupyterGrid),
    TEST_JUPYTER_GRID(venvTestJupyterGrid, VenvTypeConstants.TEST_DEPS),
    RUN_JPY(venvRunJpy),
    TEST_JPY(venvTestJpy, VenvTypeConstants.TEST_DEPS),
    TEST_DH(venvTestDh, VenvTypeConstants.TEST_DEPS),
    PY_DOCS(venvPyDocs, ['beautifulsoup4', 'lxml']),
    RUN_DH(venvRunDh),
    RUN_DH_JUPYTER(venvRunDhJupyter),
    RUN_JUPYTER_NOTEBOOK_SERVER(venvRunDhJupyterNotebookServer),
    RUN_NBFORMAT(venvRunNbformat, ['nbformat']),
    RUN_SPHINX(venvSphinx, ['sphinx']),
    ;

    final String venvName
    final List<String> additional

    VenvType(String name, List<String> additional=[]) {
        this.venvName = name
        this.additional = additional
    }

    /**
     * @return Additional dependencies to pip install when creating the venv.
     *
     * We will glue on locally built wheels after the initial environment is setup.
     * Feel free to override this method in an enum member to customize (or add an optional constructor parameter).
     *
     * IDO-289 :
     * We may also want to add an additional abstraction layer around venv creation,
     * such that creating the venv _and_ all invocations of pip happen in a single task.
     * This will enable us to setup build caching on all venvs, so we can just pull them out of cache.
     *
     */
    List<String> getPypiDependencies() {
        // TODO: whenever the result of this method changes, we need to blow away the venv,
        //   or else the jetbrains python plugin will not update the venv appropriately
        return additional
   }
}
