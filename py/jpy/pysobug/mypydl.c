#include <Python.h>

int mypy_run(int argc, const char** argv)
{
	const char* script;
	int i, status;

	Py_Initialize();

	for (i = 0; i < argc; i++) {
		script = argv[i];
		printf("mypy: executing [%s]\n", script);
		status = PyRun_SimpleString(script);
		printf("mypy: status %d\n", status);
	}

	Py_Finalize();

	return 0;
}

