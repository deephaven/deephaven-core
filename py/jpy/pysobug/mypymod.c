#include <Python.h>


PyObject* id(PyObject* self, PyObject* args);


static PyMethodDef functions[] = 
{
    {"id", id, METH_VARARGS, "id()"},
    {NULL, NULL, 0, NULL} /*Sentinel*/
};


static struct PyModuleDef module_def =
{
    PyModuleDef_HEAD_INIT,
    "mypymod",   /* Name */
    "My Python Module",   /* Module documentation */
    -1,                /* Size of per-interpreter state  */
    functions,   
    NULL,     // m_reload
    NULL,     // m_traverse
    NULL,     // m_clear
    NULL      // m_free
};


PyMODINIT_FUNC PyInit_mypymod(void)
{
    PyObject* module = NULL;

    printf("PyInit_mypymod: enter\n");

    module = PyModule_Create(&module_def);
    if (module == NULL) {
        return NULL;
    }

    printf("PyInit_mypymod: exit: module=%p\n", module);

    return module;
}


PyObject* id(PyObject* self, PyObject* args)
{
	static int idval = 0;
	idval++;
	return Py_BuildValue("i", idval);
}


