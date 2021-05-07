#include <stdio.h>
#include <stdlib.h>
#include <dlfcn.h>

typedef int (*mypy_run_fn)(int argc, const char** argv);

int main(int argc, const char** argv) 
{
	void* handle;
	mypy_run_fn mypy_run;

    // This one works
	handle = dlopen("./mypydl.so", RTLD_LAZY | RTLD_GLOBAL);
	// This one not
	//handle = dlopen("./mypydl.so", RTLD_LAZY);
	if (handle == NULL) {
		fprintf(stderr, "mypy: error: %s\n", dlerror());
		return 1;	
	}

	mypy_run = (mypy_run_fn) dlsym(handle, "mypy_run");
	if (mypy_run == NULL) {
		fprintf(stderr, "mypy: error: %s\n", dlerror());
		return 2;	
	}

	mypy_run(argc - 1, argv + 1);

	dlclose(handle);
	return 0;
}
